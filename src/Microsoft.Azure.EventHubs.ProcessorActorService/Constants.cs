using Microsoft.ServiceFabric.Actors;
using System;


namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    class Constants
    {
        internal static readonly ActorId MapperActorId = new ActorId("Mapper");
        internal static readonly string MetricReporterActorIdPrefix = "MetricReporter-";
        internal static readonly string RoundRobinActorIdPrefix = "RoundRobin-";
        internal static readonly string PersistorActorIdPrefix = "Persistor-";

        internal static readonly string UserLoadMetricName = "UserMetric";
        internal static readonly string PrimaryReplicaCountMetricName = "PrimaryCount";
        internal static readonly TimeSpan LoadMetricReportingInterval = TimeSpan.FromMinutes(5.0);

        internal static readonly TimeSpan UpdateServiceDescriptionTimeout = TimeSpan.FromSeconds(30.0);

        internal static readonly TimeSpan FinishUserCallsOnCloseTimeout = TimeSpan.FromSeconds(10.0);

        internal static readonly string ConfigurationPackageObjectName = "Config";
        internal static readonly string EventProcessorConfigSectionName = "EventProcessorConfig";
        internal static readonly string EventHubConnectionStringName = "EventHubConnectionString";
        internal static readonly string EventHubConsumerGroupName = "EventHubConsumerGroup";
        internal static readonly string EventHubConsumerGroupDefault = PartitionReceiver.DefaultConsumerGroupName;
        internal static readonly string MaxConcurrentActorCallsName = "MaxConcurrentActorCalls";
        internal static readonly int MaxConcurrentActorCallsDefault = 1;
        internal static readonly string SessionNamePropertyName = "SessionNameProperty";
        internal static readonly string SessionNamePropertyDefault = null;

        internal static readonly string MissingSessionName = "MissingSessionName";
        internal static readonly string NotUsingSessions = "NotUsingSessions";

        internal static readonly int DefaultMaxBatchSize = 100;

        internal static readonly string MapperStatePrefix = "mapper:";
        internal static readonly string MapperActorMappingPrefix = Constants.MapperStatePrefix + "mapping:";
        internal static readonly string MapperNextAllocPrefix = Constants.MapperStatePrefix + "nextalloc:";
        internal static readonly string PartitionStatePrefix = "partition:";
        internal static readonly string PartitionRoundRobinActorsPrefix = Constants.PartitionStatePrefix + "roundrobin:";
        internal static readonly string PersistorStatePrefix = "persistor:";
        internal static readonly string CheckpointMapPrefix = PersistorStatePrefix + "checkpointmap:";
    }
}
