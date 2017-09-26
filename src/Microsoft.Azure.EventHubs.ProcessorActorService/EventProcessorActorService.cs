using Microsoft.ServiceFabric.Actors.Runtime;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors;
using System.Fabric;
using System.Threading;
using System.Fabric.Description;

namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    class EventProcessorActorService : ActorService
    {
        private bool metricLoopContinue = false;
        private ReplicaRole currentRole = ReplicaRole.Unknown;

        private string eventHubConnectionString;
        private string eventHubConsumerGroup;

        private EventHubClient ehClient = null;
        private PartitionReceiver ehReceiver = null;

        private bool isReceivingPartition = false;
        private string eventHubPartitionId;

        public EventProcessorActorService(StatefulServiceContext context, ActorTypeInformation actorTypeInfo,
            Func<ActorService, ActorId, ActorBase> actorFactory = null, Func<ActorBase, IActorStateProvider, IActorStateManager> stateManagerFactory = null,
            IActorStateProvider stateProvider = null, ActorServiceSettings settings = null) :
            base(context, actorTypeInfo, actorFactory, stateManagerFactory, stateProvider, settings)
        {
        }

        protected async override Task OnOpenAsync(ReplicaOpenMode openMode, CancellationToken cancellationToken)
        {
            await base.OnOpenAsync(openMode, cancellationToken);

            EventProcessorActorService.ServiceName = this.Context.ServiceName;
            EventProcessorActorService.ServiceContext = this.Context;

            this.eventHubConnectionString = ServiceUtilities.GetConfigurationValue(Constants.EventHubConnectionStringName);
            if (this.eventHubConnectionString == null)
            {
                throw new Exception("Service configuration must specify EventHubConnectionString");
            }
            this.eventHubConsumerGroup = ServiceUtilities.GetConfigurationValue(Constants.EventHubConsumerGroupName, Constants.EventHubConsumerGroupDefault);

            this.PartitionOrdinal = await ServiceUtilities.GetServicePartitionOrdinal(this.Partition.PartitionInfo, cancellationToken);
            if (this.PartitionOrdinal < 0)
            {
                throw new Exception("Could not determine partition ordinal for this service partition");
            }

            this.ehClient = EventHubClient.CreateFromConnectionString(this.eventHubConnectionString);
            EventHubRuntimeInformation ehInfo = await ehClient.GetRuntimeInformationAsync(); // TODO exceptions
            int servicePartitionCount = await ServiceUtilities.GetServicePartitionCount(cancellationToken);
            if (ehInfo.PartitionCount > servicePartitionCount)
            {
                throw new Exception(String.Format("Event hub has {0} partitions but service only has {1}, service must have at least {0} partitions",
                    ehInfo.PartitionCount, await ServiceUtilities.GetServicePartitionCount(cancellationToken)));
            }
            this.NonReceivingPartitionsPresent = servicePartitionCount > ehInfo.PartitionCount;
            this.isReceivingPartition = this.PartitionOrdinal < ehInfo.PartitionCount;
            if (this.isReceivingPartition)
            {
                this.eventHubPartitionId = ehInfo.PartitionIds[this.PartitionOrdinal];
            }
        }

        protected async override Task OnCloseAsync(CancellationToken cancellationToken)
        {
            await StopReceiving();

            if (this.ehClient != null)
            {
                await this.ehClient.CloseAsync();
                this.ehClient = null;
            }

            await base.OnCloseAsync(cancellationToken);
        }

        protected async override Task OnChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            await base.OnChangeRoleAsync(newRole, cancellationToken);

            if ((newRole == ReplicaRole.Primary) && (this.currentRole != ReplicaRole.Primary))
            {
                // Replica transitioning to primary -- start receiving
                await StartReceiving(cancellationToken);

                // Stateful Actor Service does not provide access to service description at create time to add metrics,
                // so we have to update the service description later. Repeatedly adding the same metrics should be idempotent,
                // but it's also inefficient.
                //
                // There will always be a partition 0, so do it only from partition 0; only when a partition 0 replica is promoted to
                // primary (to avoid all three partition 0 replicas from doing it); and even then only if the user load metric is not
                // already present. It is still possible that there could be races, but these measures will greatly reduce how many
                // times UpdateServiceAsync is called.
                if (this.PartitionOrdinal == 0)
                {
                    FabricClient fc = new FabricClient();
                    ServiceDescription sd = await fc.ServiceManager.GetServiceDescriptionAsync(this.Context.ServiceName,
                        Constants.UpdateServiceDescriptionTimeout, cancellationToken);
                    bool addMetrics = true;

                    if (sd.Metrics.Count > 0)
                    {
                        foreach (ServiceLoadMetricDescription metric in sd.Metrics)
                        {
                            if (metric.Name.CompareTo(Constants.UserLoadMetricName) == 0)
                            {
                                addMetrics = false;
                                break;
                            }
                        }
                    }

                    if (addMetrics)
                    {
                        StatefulServiceUpdateDescription ssud = new StatefulServiceUpdateDescription();

                        StatefulServiceLoadMetricDescription primaryCountMetric = new StatefulServiceLoadMetricDescription();
                        primaryCountMetric.Name = Constants.PrimaryReplicaCountMetricName;
                        primaryCountMetric.PrimaryDefaultLoad = 1;
                        primaryCountMetric.SecondaryDefaultLoad = 0;
                        primaryCountMetric.Weight = ServiceLoadMetricWeight.Medium;
                        ssud.Metrics.Add(primaryCountMetric);

                        StatefulServiceLoadMetricDescription userLoadMetric = new StatefulServiceLoadMetricDescription();
                        userLoadMetric.Name = Constants.UserLoadMetricName;
                        userLoadMetric.PrimaryDefaultLoad = 0;
                        userLoadMetric.SecondaryDefaultLoad = 0;
                        userLoadMetric.Weight = ServiceLoadMetricWeight.High;
                        ssud.Metrics.Add(userLoadMetric);

                        await fc.ServiceManager.UpdateServiceAsync(this.Context.ServiceName, ssud, Constants.UpdateServiceDescriptionTimeout, cancellationToken);
                    }
                }
            }
            else if ((newRole != ReplicaRole.Primary) && (this.currentRole == ReplicaRole.Primary))
            {
                // Replica transitioning from primary -- stop receiving
                await StopReceiving();
            }
            // else some other transition, don't care
        }

        // Safe to call for any partition. No-op for non-receiving partitions.
        private async Task StartReceiving(CancellationToken cancellationToken)
        {
            if (this.isReceivingPartition)
            {
                IUserActor persistorActor = await ServiceUtilities.GetPersistorForPartition(this.Partition.PartitionInfo, cancellationToken);

                Dictionary<string, CompletionTracker> startingCheckpoints = await persistorActor.GetAllCheckpoints(cancellationToken);
                Tuple<string, long> startingPoint = new Tuple<string, long>(PartitionReceiver.StartOfStream, long.MaxValue);
                foreach (KeyValuePair<string, CompletionTracker> kvp in startingCheckpoints)
                {
                    Tuple<string, long> candidateLowest = kvp.Value.GetLowestUncompletedOffsetAndSequenceNumber();
                    if (candidateLowest.Item2 < startingPoint.Item2)
                    {
                        startingPoint = candidateLowest;
                    }
                }

                // Use an epoch receiver to establish exclusivity, but Service Fabric framework means we don't have the problem of
                // multiple hosts racing to be the owner of the event hub partition at startup time. Epoch receiver is still useful to kick
                // off the previous receiver if the node crashed (service thinks the receiver is still open) or if the Service Fabric
                // role transition to secondary is a bit slow, but the winner should always be the most recent receiver. So we
                // can use a fixed epoch of 0 which is simpler than trying to persist and increment an epoch value.
                this.ehReceiver = this.ehClient.CreateEpochReceiver(this.eventHubConsumerGroup, this.eventHubPartitionId,
                    startingPoint.Item1, 0); // TODO exceptions?
                this.ehReceiver.SetReceiveHandler(new ReceiveHandler(this, startingCheckpoints, cancellationToken)); // TODO exceptions?
            }
        }

        // Safe to call regardless of whether this replica/partition is receiving or not.
        private async Task StopReceiving()
        {
            if (this.ehReceiver != null)
            {
                this.ehReceiver.SetReceiveHandler(null); // TODO exceptions?
                await this.ehReceiver.CloseAsync();
                this.ehReceiver = null;
            }
        }

        protected async override Task RunAsync(CancellationToken cancellationToken)
        {
            // The metric reporting loop will be running on all replicas regardless of role, but only
            // does anything if the replica is primary. For others, it's a sleep and a no-op, consuming
            // minimal resources. This is a lot simpler than starting and stopping it on role change.
            await MetricReportingLoop(cancellationToken);
        }

        private async Task MetricReportingLoop(CancellationToken cancellationToken)
        { 
            IUserActor reporterActor = await ServiceUtilities.GetMetricReporterForPartition(this.Partition.PartitionInfo, cancellationToken);

            while (!cancellationToken.IsCancellationRequested && this.metricLoopContinue)
            {
                await Task.Delay(Constants.LoadMetricReportingInterval, cancellationToken);

                // TODO ActiveSecondary replica could do load reporting?
                if (this.currentRole == ReplicaRole.Primary)
                {
                    int load = await reporterActor.GetAndClearAggregatedLoadMetric();

                    this.Partition.ReportLoad(new List<LoadMetric> { new LoadMetric(Constants.UserLoadMetricName, load) });
                }
            }
        }

        internal int PartitionOrdinal
        {
            get; private set;
        }

        internal ServicePartitionInformation PartitionInfo
        {
            get { return this.Partition.PartitionInfo; }
        }

        internal bool NonReceivingPartitionsPresent
        {
            get; private set;
        }

        internal static Uri ServiceName
        {
            get; private set;
        }

        internal static StatefulServiceContext ServiceContext
        {
            get; private set;
        }
    }
}
