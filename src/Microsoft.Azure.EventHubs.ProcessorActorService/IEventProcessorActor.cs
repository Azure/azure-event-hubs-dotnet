using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    public interface IEventProcessorActor : IActor
    {
        #region Mapper
        // Called on: Mapper actor
        // Called by: service code
        Task<ActorId> GetOrAddActorInPartition(string stringId, ServicePartitionInformation targetPartitionInfo, CancellationToken cancellationToken);

        // Called on: Mapper actor
        // Called by: user actors
        Task<ActorId> GetExistingActor(string stringId, CancellationToken cancellationToken);
        #endregion

        #region LoadMetrics
        // Called on: user actor
        // Called by: user actor implementation
        Task ReportLoadMetric(int load, CancellationToken cancellationToken);

        // Called on: MetricsReporter actor
        // Called by: ReportLoadMetric() on user actor
        Task AggregateLoadMetric(int load);

        // Called on: MetricsReporter actor
        // Called by: service code
        Task<int> GetAndClearAggregatedLoadMetric();
        #endregion

        #region Persistor
        //
        // General persistence methods
        //

        // Called on: Persistor actor
        // Called by: service code
        Task<bool> IsPersisted(string key, CancellationToken cancellationToken);

        // Called on: Persistor actor
        // Called by: service code and Persistor actor
        Task<ConditionalValue<T>> TryGetState<T>(string key, CancellationToken cancellationToken);

        // Called on: Persistor actor
        // Called by: service code and Persistor actor
        Task SetState<T>(string key, T value, CancellationToken cancellationToken);

        //
        // Checkpointing methods for use by user actor implementations
        //

        // Called on: user actor
        // Called by: user actor implementation
        Task CheckpointSingleEvent(EventData e, CancellationToken cancellationToken);

        // Called on: user actor
        // Called by: user actor implementation
        Task CheckpointAt(EventData e, CancellationToken cancellationToken);

        // Called on: Persistor actor
        // Called by: service code
        Task EventDispatched(EventData e, string sessionName, CancellationToken cancellationToken);

        // Called on: Persistor actor
        // Called by: Checkpoint* on user actor
        Task MarkCompleted(EventData e, bool andPreceding, string sessionName, CancellationToken cancellationToken);

        // Called on: Persistor actor
        // Called by: service code
        Task<Dictionary<string, CompletionTracker>> GetAllCheckpoints(CancellationToken cancellationToken);
        #endregion
    }
}
