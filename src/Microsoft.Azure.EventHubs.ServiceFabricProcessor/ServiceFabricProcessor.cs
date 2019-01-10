// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Fabric.Description;
    using System.Fabric.Query;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// Base class that implements event processor functionality.
    /// </summary>
    public class ServiceFabricProcessor : IPartitionReceiveHandler
    {
        // Service Fabric objects initialized in constructor
        private readonly IReliableStateManager ServiceStateManager;
        private readonly StatefulServiceContext ServiceContext;
        private readonly IStatefulServicePartition ServicePartition;

        // ServiceFabricProcessor settings initialized in constructor
        private readonly IEventProcessor userEventProcessor;
        private readonly EventProcessorOptions options;
        private readonly ICheckpointMananger checkpointManager;

        // Initialized during RunAsync startup
        private int fabricPartitionOrdinal = -1;
        private int servicePartitions = -1;
        private string hubPartitionId;
        private PartitionContext partitionContext;
        private string initialOffset;
        private CancellationTokenSource internalCanceller;
        private Exception internalFatalException;
        private CancellationToken linkedCancellationToken;
        private EventHubsConnectionStringBuilder ehConnectionString;
        private string consumerGroupName;

        // Value managed by RunAsync
        private int running = 0;


        /// <summary>
        /// Constructor. Arguments break down into three groups: (1) Service Fabric objects so this library can access
        /// Service Fabric facilities, (2) Event Hub-related arguments which indicate what event hub to receive from and
        /// how to process the events, and (3) advanced, which right now consists only of the ability to replace the default
        /// reliable dictionary-based checkpoint manager with a user-provided implementation.
        /// </summary>
        /// <param name="context">Service Fabric-provided context structure</param>
        /// <param name="stateManager">Service Fabric-provided state manager, provides access to reliable dictionaries</param>
        /// <param name="partition">Service Fabric-provided partition information</param>
        /// <param name="userEventProcessor">User's event processor implementation</param>
        /// <param name="eventHubConnectionString">Connection string for user's event hub</param>
        /// <param name="eventHubConsumerGroup">Optional: Name of event hub consumer group to receive from, defaults to "$Default"</param>
        /// <param name="options">Optional: Options structure for ServiceFabricProcessor library</param>
        /// <param name="checkpointManager">Very advanced/optional: user-provided checkpoint manager implementation</param>
        public ServiceFabricProcessor(StatefulServiceContext context, IReliableStateManager stateManager, IStatefulServicePartition partition, IEventProcessor userEventProcessor,
            string eventHubConnectionString, string eventHubConsumerGroup = null,
            EventProcessorOptions options = null, ICheckpointMananger checkpointManager = null)
        {
            this.ServiceContext = context;
            this.ServiceStateManager = stateManager;
            this.ServicePartition = partition;

            this.userEventProcessor = userEventProcessor;

            this.ehConnectionString = new EventHubsConnectionStringBuilder(eventHubConnectionString);
            this.consumerGroupName = eventHubConsumerGroup ?? PartitionReceiver.DefaultConsumerGroupName;

            this.options = options ?? new EventProcessorOptions();
            this.checkpointManager = checkpointManager ?? new ReliableDictionaryCheckpointMananger(this.ServiceStateManager);
            this.EventHubClientFactory = new EventHubWrappers.EventHubClientFactory();
            this.TestMode = false;
        }

        /// <summary>
        /// For testing purposes. Do not change after calling RunAsync.
        /// </summary>
        public EventHubWrappers.IEventHubClientFactory EventHubClientFactory { get; set; }

        /// <summary>
        /// For testing purposes. Do not change after calling RunAsync.
        /// </summary>
        public bool TestMode { get; set; }

        /// <summary>
        /// Starts processing of events.
        /// </summary>
        /// <param name="fabricCancellationToken">Cancellation token provided by Service Fabric, assumed to indicate instance shutdown when cancelled.</param>
        /// <returns>Task that completes when event processing shuts down.</returns>
        public async Task RunAsync(CancellationToken fabricCancellationToken)
        {
            if (Interlocked.Exchange(ref this.running, 1) == 1)
            {
                EventProcessorEventSource.Current.Message("Already running");
                throw new InvalidOperationException("EventProcessorService.RunAsync has already been called.");
            }

            this.internalCanceller = new CancellationTokenSource();
            this.internalFatalException = null;

            try
            {
                using (CancellationTokenSource linkedCanceller = CancellationTokenSource.CreateLinkedTokenSource(fabricCancellationToken, this.internalCanceller.Token))
                {
                    this.linkedCancellationToken = linkedCanceller.Token;
                    
                    await InnerRunAsync();

                    this.options.NotifyOnShutdown(null);
                }
            }
            catch (Exception e)
            {
                // If InnerRunAsync throws, that is intended to be a fatal exception for this instance.
                // Catch it here just long enough to log and notify, then rethrow.

                EventProcessorEventSource.Current.Message("THROWING OUT: {0}", e);
                this.options.NotifyOnShutdown(e);
                throw e;
            }
        }

        private async Task InnerRunAsync()
        {
            EventHubWrappers.IEventHubClient ehclient = null;
            EventHubWrappers.IPartitionReceiver receiver = null;

            try
            {
                //
                // Get Service Fabric partition information.
                //
                await GetServicePartitionId(this.linkedCancellationToken);

                //
                // Create EventHubClient and check partition count.
                //
                Exception lastException = null;
                EventProcessorEventSource.Current.Message("Creating event hub client");
                lastException = RetryWrapper(() => { ehclient = this.EventHubClientFactory.CreateFromConnectionString(this.ehConnectionString.ToString()); });
                if (ehclient == null)
                {
                    EventProcessorEventSource.Current.Message("Out of retries event hub client");
                    throw new Exception("Out of retries creating EventHubClient", lastException);
                }
                EventProcessorEventSource.Current.Message("Event hub client OK");
                EventProcessorEventSource.Current.Message("Getting event hub info");
                EventHubRuntimeInformation ehInfo = null;
                lastException = RetryWrapper(async () => { ehInfo = await ehclient.GetRuntimeInformationAsync(); });
                if (ehInfo == null)
                {
                    EventProcessorEventSource.Current.Message("Out of retries getting event hub info");
                    throw new Exception("Out of retries getting event hub runtime info", lastException);
                }
                if (this.TestMode)
                {
                    if (this.servicePartitions > ehInfo.PartitionCount)
                    {
                        EventProcessorEventSource.Current.Message("TestMode requires event hub partition count larger than service partitinon count");
                        throw new EventProcessorConfigurationException("TestMode requires event hub partition count larger than service partitinon count");
                    }
                    else if (this.servicePartitions < ehInfo.PartitionCount)
                    {
                        EventProcessorEventSource.Current.Message("TestMode: receiving from subset of event hub");
                    }
                }
                else if (ehInfo.PartitionCount != this.servicePartitions)
                {
                    EventProcessorEventSource.Current.Message($"Service partition count {this.servicePartitions} does not match event hub partition count {ehInfo.PartitionCount}");
                    throw new EventProcessorConfigurationException($"Service partition count {this.servicePartitions} does not match event hub partition count {ehInfo.PartitionCount}");
                }
                this.hubPartitionId = ehInfo.PartitionIds[this.fabricPartitionOrdinal];

                //
                // Generate a PartitionContext now that the required info is available.
                //
                this.partitionContext = new PartitionContext(this.linkedCancellationToken, this.hubPartitionId, this.ehConnectionString.EntityPath, this.consumerGroupName, this.checkpointManager);

                //
                // Start up checkpoint manager.
                //
                await CheckpointStartup(this.linkedCancellationToken);

                //
                // If there was a checkpoint, the offset is in this.initialOffset, so convert it to an EventPosition.
                // If no checkpoint, get starting point from user-supplied provider.
                //
                EventPosition initialPosition = null;
                if (this.initialOffset != null)
                {
                    EventProcessorEventSource.Current.Message($"Initial position from checkpoint, offset {this.initialOffset}");
                    initialPosition = EventPosition.FromOffset(this.initialOffset);
                }
                else
                {
                    initialPosition = this.options.InitialPositionProvider(this.hubPartitionId);
                    EventProcessorEventSource.Current.Message("Initial position from provider");
                }

                //
                // Create receiver.
                //
                EventProcessorEventSource.Current.Message("Creating receiver");
                lastException = RetryWrapper(() => { receiver = ehclient.CreateEpochReceiver(this.consumerGroupName, this.hubPartitionId, initialPosition, this.initialOffset,
                    Constants.FixedReceiverEpoch, this.options.ClientReceiverOptions); });
                if (receiver == null)
                {
                    EventProcessorEventSource.Current.Message("Out of retries creating receiver");
                    throw new Exception("Out of retries creating event hub receiver", lastException);
                }

                //
                // Call Open on user's event processor instance.
                // If user's Open code fails, treat that as a fatal exception and let it throw out.
                //
                EventProcessorEventSource.Current.Message("Creating event processor");
                await this.userEventProcessor.OpenAsync(this.linkedCancellationToken, this.partitionContext);
                EventProcessorEventSource.Current.Message("Event processor created and opened OK");

                //
                // Start metrics reporting. This runs as a separate background thread.
                //
                Thread t = new Thread(this.MetricsHandler);
                t.Start();

                //
                // Receive pump.
                //
                EventProcessorEventSource.Current.Message("RunAsync setting handler and waiting");
                this.MaxBatchSize = this.options.MaxBatchSize;
                receiver.SetReceiveHandler(this, this.options.InvokeProcessorAfterReceiveTimeout);
                this.linkedCancellationToken.WaitHandle.WaitOne();

                EventProcessorEventSource.Current.Message("RunAsync continuing, cleanup");
            }
            finally
            {
                if (this.partitionContext != null)
                {
                    await this.userEventProcessor.CloseAsync(this.partitionContext, this.linkedCancellationToken.IsCancellationRequested ? CloseReason.Cancelled : CloseReason.Failure);
                }
                if (receiver != null)
                {
                    receiver.SetReceiveHandler(null);
                    await receiver.CloseAsync();
                }
                if (ehclient != null)
                {
                    await ehclient.CloseAsync();
                }
                if (this.internalFatalException != null)
                {
                    throw this.internalFatalException;
                }
            }
        }

        private EventHubsException RetryWrapper(Action action)
        {
            EventHubsException lastException = null;

            for (int i = 0; i < Constants.RetryCount; i++)
            {
                this.linkedCancellationToken.ThrowIfCancellationRequested();
                try
                {
                    action.Invoke();
                    break;
                }
                catch (EventHubsException e)
                {
                    if (!e.IsTransient)
                    {
                        throw e;
                    }
                    lastException = e;
                }
            }

            return lastException;
        }

        /// <summary>
        /// From IPartitionReceiveHandler
        /// </summary>
        public int MaxBatchSize { get; set; }

        async Task IPartitionReceiveHandler.ProcessEventsAsync(IEnumerable<EventData> events)
        {
            IEnumerable<EventData> effectiveEvents = events ?? new List<EventData>(); // convert to empty list if events is null

            if (events != null)
            {
                // Save position of last event if we got a real list of events
                IEnumerator<EventData> scanner = effectiveEvents.GetEnumerator();
                EventData last = null;
                while (scanner.MoveNext())
                {
                    last = scanner.Current;
                }
                if (last != null)
                {
                    this.partitionContext.SetOffsetAndSequenceNumber(last);
                    if (this.options.EnableReceiverRuntimeMetric)
                    {
                        this.partitionContext.RuntimeInformation.Update(last);
                    }
                }
            }

            await this.userEventProcessor.ProcessEventsAsync(this.linkedCancellationToken, this.partitionContext, effectiveEvents);

            foreach (EventData ev in effectiveEvents)
            {
                ev.Dispose();
            }
        }

        Task IPartitionReceiveHandler.ProcessErrorAsync(Exception error)
        {
            EventProcessorEventSource.Current.Message($"RECEIVE EXCEPTION on {this.hubPartitionId}: {error}");
            this.userEventProcessor.ProcessErrorAsync(this.partitionContext, error);
            if (error is EventHubsException)
            {
                if (!(error as EventHubsException).IsTransient)
                {
                    this.internalFatalException = error;
                    this.internalCanceller.Cancel();
                }
                // else don't cancel on transient errors
            }
            else
            {
                // All other exceptions are assumed fatal.
                this.internalFatalException = error;
                this.internalCanceller.Cancel();
            }
            return Task.CompletedTask;
        }

        private async Task CheckpointStartup(CancellationToken cancellationToken)
        {
            // Set up store and get checkpoint, if any.
            await this.checkpointManager.CreateCheckpointStoreIfNotExistsAsync(cancellationToken);
            Checkpoint checkpoint = await this.checkpointManager.CreateCheckpointIfNotExistsAsync(this.hubPartitionId, cancellationToken);
            if (!checkpoint.Valid)
            {
                // Not actually any existing checkpoint.
                this.initialOffset = null;
                EventProcessorEventSource.Current.Message("No checkpoint");
            }
            else if (checkpoint.Version == 1)
            {
                this.initialOffset = checkpoint.Offset;
                EventProcessorEventSource.Current.Message($"Checkpoint provides initial offset {this.initialOffset}");
            }
            else
            {
                // It's actually a later-version checkpoint but we don't know the details.
                // Access it via the V1 interface and hope it does something sensible.
                this.initialOffset = checkpoint.Offset;
                EventProcessorEventSource.Current.Message($"Unexpected checkpoint version {checkpoint.Version}, provided initial offset {this.initialOffset}");
            }
        }

        private async Task GetServicePartitionId(CancellationToken cancellationToken)
        {
            if (this.fabricPartitionOrdinal == -1)
            {
                using (FabricClient fabricClient = new FabricClient())
                {
                    ServicePartitionList partitionList =
                        await fabricClient.QueryManager.GetPartitionListAsync(this.ServiceContext.ServiceName);

                    // Set the number of partitions
                    this.servicePartitions = partitionList.Count;

                    // Which partition is this one?
                    for (int a = 0; a < partitionList.Count; a++)
                    {
                        if (partitionList[a].PartitionInformation.Id == this.ServiceContext.PartitionId)
                        {
                            this.fabricPartitionOrdinal = a;
                            break;
                        }
                    }

                    EventProcessorEventSource.Current.Message($"Total partitions {this.servicePartitions}");
                }
            }
        }

        private void MetricsHandler()
        {
            EventProcessorEventSource.Current.Message("METRIC reporter starting");

            while (!this.linkedCancellationToken.IsCancellationRequested)
            {
                Dictionary<string, int> userMetrics = this.userEventProcessor.GetLoadMetric(this.linkedCancellationToken, this.partitionContext);

                try
                {
                    List<LoadMetric> reportableMetrics = new List<LoadMetric>();
                    foreach (KeyValuePair<string, int> metric in userMetrics)
                    {
                        EventProcessorEventSource.Current.Message($"METRIC {metric.Key} for partition {this.partitionContext.PartitionId} is {metric.Value}");
                        reportableMetrics.Add(new LoadMetric(metric.Key, metric.Value));
                    }
                    this.ServicePartition.ReportLoad(reportableMetrics);
                    Task.Delay(Constants.MetricReportingInterval, this.linkedCancellationToken).Wait(); // throws on cancel
                }
                catch (Exception e)
                {
                    EventProcessorEventSource.Current.Message($"METRIC partition {this.partitionContext.PartitionId} exception {e}");
                }
            }

            EventProcessorEventSource.Current.Message("METRIC reporter exiting");
        }
    }
}
