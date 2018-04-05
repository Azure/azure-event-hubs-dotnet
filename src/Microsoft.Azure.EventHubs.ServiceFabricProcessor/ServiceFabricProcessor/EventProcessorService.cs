// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Description;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Runtime;


namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    public class EventProcessorService<TEventProcessor> : StatefulService, EventHubWrappers.IPartitionReceiveHandler2
        where TEventProcessor : IEventProcessor, new()
    {
        private PartitionContext partitionContext = null;
        private EventHubsConnectionStringBuilder ehConnectionString;
        private string consumerGroupName;
        private string partitionId = null;
        private int servicePartitions = -1;
        private string initialOffset = null;
        private CancellationTokenSource internalCanceller;
        private Exception internalFatalException = null;
        private IEventProcessor userEventProcessor = null;

        public EventProcessorService(StatefulServiceContext context)
            : base(context)
        {
            this.Options = new EventProcessorOptions();
            this.EventProcessorFactory = new DefaultEventProcessorFactory<TEventProcessor>();
            this.CheckpointManager = new ReliableDictionaryCheckpointMananger(this.StateManager);
            this.EventHubClientFactory = new EventHubWrappers.EventHubClientFactory();

            this.internalCanceller = new CancellationTokenSource();
        }

        protected EventProcessorOptions Options { get; set; }

        protected IEventProcessorFactory EventProcessorFactory { get; set; }

        protected ICheckpointMananger CheckpointManager { get; set; }

        protected EventHubWrappers.IEventHubClientFactory EventHubClientFactory { get; set; }

        sealed protected override async Task RunAsync(CancellationToken fabricCancellationToken)
        {
            try
            {
                using (CancellationTokenSource linkedCanceller = CancellationTokenSource.CreateLinkedTokenSource(fabricCancellationToken, this.internalCanceller.Token))
                {
                    await InnerRunAsync(linkedCanceller.Token);
                }
            }
            catch (Exception e)
            {
                // If InnerRunAsync throws, that is intended to be a fatal exception for this instance.
                // Catch it here just long enough to log it, then rethrow.

                EventProcessorEventSource.Current.Message("THROWING OUT: {0}", e);
                throw e;
            }
        }

        private async Task InnerRunAsync(CancellationToken linkedCancellationToken)
        {
            EventHubWrappers.IEventHubClient ehclient = null;
            EventHubWrappers.IPartitionReceiver receiver = null;

            try
            {
                //
                // General startup tasks.
                //
                await PartitionStartup(linkedCancellationToken);

                //
                // Instantiate user's event processor class and call Open.
                // If user's Open code fails, treat that as a fatal exception and let it throw out.
                //
                EventProcessorEventSource.Current.Message("Creating event processor");
                this.userEventProcessor = this.EventProcessorFactory.CreateEventProcessor(this.partitionContext);
                await this.userEventProcessor.OpenAsync(this.partitionContext);
                EventProcessorEventSource.Current.Message("Event processor created and opened OK");

                //
                // Create EventHubClient and check partition count.
                //
                Exception lastException = null;
                EventProcessorEventSource.Current.Message("Creating event hub client");
                for (int i = 0; i < Constants.RetryCount; i++)
                {
                    linkedCancellationToken.ThrowIfCancellationRequested();
                    try
                    {
                        ehclient = this.EventHubClientFactory.CreateFromConnectionString(this.ehConnectionString.ToString());
                        break;
                    }
                    catch (EventHubsException e)
                    {
                        if (!e.IsTransient)
                        {
                            // Nontransient exceptions when creating the client are fatal and throw out of RunAsync.
                            throw e;
                        }
                        lastException = e;
                    }
                }
                if (ehclient == null)
                {
                    EventProcessorEventSource.Current.Message("Out of retries event hub client");
                    throw new Exception("Out of retries creating EventHubClient", lastException);
                }
                EventProcessorEventSource.Current.Message("Event hub client OK");
                EventProcessorEventSource.Current.Message("Getting event hub info");
                EventHubRuntimeInformation ehInfo = null;
                for (int i = 0; i < Constants.RetryCount; i++)
                {
                    linkedCancellationToken.ThrowIfCancellationRequested();
                    try
                    {
                        ehInfo = await ehclient.GetRuntimeInformationAsync();
                        break;
                    }
                    catch (EventHubsException e)
                    {
                        if (!e.IsTransient)
                        {
                            // Nontransient exceptions here are fatal and throw out of RunAsync.
                            throw e;
                        }
                        lastException = e;
                    }
                }
                if (ehInfo == null)
                {
                    EventProcessorEventSource.Current.Message("Out of retries getting event hub info");
                    throw new Exception("Out of retries getting event hub runtime info", lastException);
                }
                if (ehInfo.PartitionCount != this.servicePartitions)
                {
                    EventProcessorEventSource.Current.Message("Service partition count {0} does not match event hub partition count {1}", this.servicePartitions, ehInfo.PartitionCount);
                    throw new EventProcessorConfigurationException("Service partition count " + this.servicePartitions + " does not match event hub partition count " + ehInfo.PartitionCount);
                }

                //
                // If there was a checkpoint, the offset is in this.initialOffset, so convert it to an EventPosition.
                // If no checkpoint, get starting point from user-supplied provider.
                //
                EventPosition initialPosition = null;
                if (this.initialOffset != null)
                {
                    EventProcessorEventSource.Current.Message("Initial position from checkpoint, offset {0}", this.initialOffset);
                    initialPosition = EventPosition.FromOffset(this.initialOffset);
                }
                else
                {
                    initialPosition = this.Options.InitialOffsetProvider(this.partitionId);
                    EventProcessorEventSource.Current.Message("Initial position from provider");
                }

                //
                // Create receiver.
                //
                EventProcessorEventSource.Current.Message("Creating receiver");
                for (int i = 0; i < Constants.RetryCount; i++)
                {
                    linkedCancellationToken.ThrowIfCancellationRequested();
                    try
                    {
                        receiver = ehclient.CreateEpochReceiver(this.consumerGroupName, this.partitionId, initialPosition, Constants.FixedReceiverEpoch, null); // FOO receiveroptions
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
                if (receiver == null)
                {
                    EventProcessorEventSource.Current.Message("Out of retries creating receiver");
                    throw new Exception("Out of retries creating event hub receiver", lastException);
                }

                //
                // Start metrics reporting.
                //
                Task.Run(() => MetricsHandler(linkedCancellationToken));

                //
                // Receive pump.
                //
                EventProcessorEventSource.Current.Message("RunAsync setting handler and waiting");
                receiver.SetReceiveHandler(this, Options.InvokeProcessorAfterReceiveTimeout);
                linkedCancellationToken.WaitHandle.WaitOne();

                EventProcessorEventSource.Current.Message("RunAsync continuing, cleanup");
            }
            finally
            {
                if (receiver != null)
                {
                    receiver.SetReceiveHandler(null);
                    await receiver.CloseAsync();
                }
                if (ehclient != null)
                {
                    await ehclient.CloseAsync();
                }
                if (this.userEventProcessor != null)
                {
                    // partitionContext is set up before processor is created, so it is available if processor is not null
                    await this.userEventProcessor.CloseAsync(this.partitionContext, linkedCancellationToken.IsCancellationRequested ? CloseReason.Cancelled : CloseReason.Failure);
                }
                if (this.internalFatalException != null)
                {
                    throw this.internalFatalException;
                }
            }
        }

        public async Task ProcessEventsAsync(IEnumerable<EventHubWrappers.IEventData> events)
        {
            if ((events != null) || ((events == null) && Options.InvokeProcessorAfterReceiveTimeout))
            {
                IEnumerable<EventHubWrappers.IEventData> effectiveEvents = events;

                if (effectiveEvents != null)
                {
                    // Save position of last event
                    IEnumerator<EventHubWrappers.IEventData> scanner = effectiveEvents.GetEnumerator();
                    EventHubWrappers.IEventData last = null;
                    while (scanner.MoveNext())
                    {
                        last = scanner.Current;
                    }
                    this.partitionContext.SetOffsetAndSequenceNumber(last);
                }
                else
                {
                    // Client returns null on timeout, but processor expects empty enumerable.
                    effectiveEvents = new List<EventHubWrappers.IEventData>();
                }

                IEventProcessor capturedEventProcessor = this.userEventProcessor;
                if (capturedEventProcessor != null)
                {
                    await capturedEventProcessor.ProcessEventsAsync(this.partitionContext, effectiveEvents);
                }
            }
        }

        public Task ProcessErrorAsync(Exception error)
        {
            EventProcessorEventSource.Current.Message("RECEIVE EXCEPTION on {0}: {1}", this.partitionId, error);
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
                this.internalCanceller.Cancel();
            }
            return Task.CompletedTask;
        }

        private async Task PartitionStartup(CancellationToken cancellationToken)
        {
            // What partition is this? What is the total count of partitions?
            await GetServicePartitionId(cancellationToken);

            // Metrics settings are service-wide. There will always be a partition 0, so let 0 do it and other partitions can skip.
            if (this.partitionId.CompareTo("0") == 0)
            {
                await SetupMetrics(cancellationToken);
            }

            // Get event hub connection string from configuration. This is mandatory, cannot proceed without.
            string rawConnectionString = GetConfigurationValue(Constants.EventHubConnectionStringConfigName, null);
            if (rawConnectionString == null)
            {
                throw new EventProcessorConfigurationException("Event hub connection string not supplied in configuration section " + Constants.EventProcessorConfigSectionName);
            }
            EventProcessorEventSource.Current.Message("Event hub connection string {0}", rawConnectionString);
            this.ehConnectionString = new EventHubsConnectionStringBuilder(rawConnectionString);

            // Get consumer group name. Many users will be using the default consumer group, so for convenience default to that if not supplied.
            this.consumerGroupName = GetConfigurationValue(Constants.EventHubConsumerGroupConfigName, Constants.EventHubConsumerGroupConfigDefault);
            EventProcessorEventSource.Current.Message("Consumer group {0}", this.consumerGroupName);

            // Generate a PartitionContext now that the required info is available.
            this.partitionContext = new PartitionContext(cancellationToken, this.partitionId, this.ehConnectionString.EntityPath, this.consumerGroupName, this.CheckpointManager);

            // Set up store and get checkpoint, if any.
            await this.CheckpointManager.CreateCheckpointStoreIfNotExistsAsync(cancellationToken);
            Checkpoint checkpoint = await this.CheckpointManager.CreateCheckpointIfNotExistsAsync(this.partitionId, cancellationToken);
            if (!checkpoint.Valid)
            {
                // Not actually any existing checkpoint.
                this.initialOffset = null;
                EventProcessorEventSource.Current.Message("No checkpoint");
            }
            else if (checkpoint.Version == 1)
            {
                this.initialOffset = checkpoint.Offset;
                EventProcessorEventSource.Current.Message("Checkpoint says to start at {0}", this.initialOffset);
            }
            else
            {
                // It's actually a later-version checkpoint but we don't know the details.
                // Access it via the V1 interface and hope it does something sensible.
                this.initialOffset = checkpoint.Offset;
                EventProcessorEventSource.Current.Message("Checkpoint version error");
            }
        }

        private async Task GetServicePartitionId(CancellationToken cancellationToken)
        {
            if (this.partitionId == null)
            {
                Int64RangePartitionInformation thisPartition = (Int64RangePartitionInformation)this.Partition.PartitionInfo;

                ServicePartitionResolver resolver = ServicePartitionResolver.GetDefault();
                Int64RangePartitionInformation scanner = null;
                long lowScan = long.MinValue;
                int ordinal = 0;
                int totalPartitions = 0;
                do
                {
                    ServicePartitionKey resolveKey = new ServicePartitionKey(lowScan);
                    ResolvedServicePartition partition = await resolver.ResolveAsync(this.Context.ServiceName, resolveKey, cancellationToken);
                    scanner = (Int64RangePartitionInformation)partition.Info;
                    lowScan = scanner.HighKey + 1;
                    if (scanner.LowKey == thisPartition.LowKey)
                    {
                        ordinal = totalPartitions;
                        EventProcessorEventSource.Current.Message("Found our partition, ordinal {0}", ordinal);
                    }
                    totalPartitions++;
                } while (scanner.HighKey != long.MaxValue);

                this.partitionId = ordinal.ToString();
                this.servicePartitions = totalPartitions;
                EventProcessorEventSource.Current.Message("Total partitions {0}", this.servicePartitions);
            }
        }

        private string GetConfigurationValue(string configurationValueName, string defaultValue = null)
        {
            string value = defaultValue;
            ConfigurationPackage configurationPackage = this.Context.CodePackageActivationContext.GetConfigurationPackageObject(Constants.ConfigurationPackageName);
            try
            {
                ConfigurationSection configurationSection = configurationPackage.Settings.Sections[Constants.EventProcessorConfigSectionName];
                ConfigurationProperty configurationProperty = configurationSection.Parameters[configurationValueName];
                value = configurationProperty.Value;
            }
            catch (KeyNotFoundException)
            {
                // If the user has not specified a value in config, drop through and return the default value.
                // If the caller cannot continue without a user-supplied value, it is up to the caller to detect and handle.
            }
            //catch (ArgumentNullException) if configurationValueName is null, that's a code bug, do not catch
            return value;
        }

        private async Task SetupMetrics(CancellationToken cancellationToken)
        {
            FabricClient fc = new FabricClient();

            ServiceDescription sd = await fc.ServiceManager.GetServiceDescriptionAsync(this.Context.ServiceName, Constants.UpdateServiceDescriptionTimeout, cancellationToken);

            bool addMetric = true;
            if (sd.Metrics.Count > 0)
            {
                foreach (ServiceLoadMetricDescription metric in sd.Metrics)
                {
                    if (metric.Name.CompareTo(Constants.UserLoadMetricName) == 0)
                    {
                        // Metric already added -- this is probably a restart of partition 0.
                        addMetric = false;
                        break;
                    }
                }
            }

            if (addMetric)
            {
                StatefulServiceUpdateDescription ssud = new StatefulServiceUpdateDescription();

                StatefulServiceLoadMetricDescription userLoadMetric = new StatefulServiceLoadMetricDescription();
                userLoadMetric.Name = Constants.UserLoadMetricName;
                userLoadMetric.PrimaryDefaultLoad = 1;
                userLoadMetric.SecondaryDefaultLoad = 0;
                userLoadMetric.Weight = ServiceLoadMetricWeight.High;

                ssud.Metrics = sd.Metrics;
                ssud.Metrics.Add(userLoadMetric);

                await fc.ServiceManager.UpdateServiceAsync(this.Context.ServiceName, ssud, Constants.UpdateServiceDescriptionTimeout, cancellationToken);

                EventProcessorEventSource.Current.Message("METRIC added");
            }
            else
            {
                EventProcessorEventSource.Current.Message("METRIC already present");
            }
        }

        private void MetricsHandler(CancellationToken linkedCancellationToken)
        {
            EventProcessorEventSource.Current.Message("METRIC reporter starting");

            IEventProcessor capturedProcessor = this.userEventProcessor;
            while (!linkedCancellationToken.IsCancellationRequested)
            {
                int userMetric = capturedProcessor.GetLoadMetric(this.partitionContext);
                EventProcessorEventSource.Current.Message("METRIC partition {0} is {1}", this.partitionContext.PartitionId, userMetric);

                try
                {
                    this.Partition.ReportLoad(new List<LoadMetric>() { new LoadMetric(Constants.UserLoadMetricName, userMetric) });
                    Task.Delay(Constants.MetricReportingInterval, linkedCancellationToken).Wait(); // throws on cancel
                }
                catch (Exception e)
                {
                    EventProcessorEventSource.Current.Message("METRIC partition {0} exception {1}", this.partitionContext.PartitionId, e);
                }
            }

            EventProcessorEventSource.Current.Message("METRIC reporter exiting");
        }
    }
}
