using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    /// <summary>
    /// 
    /// </summary>
    public interface IEventProcessorActor : IActor
    {
        #region Mapper
        /// <summary>
        /// Called on: Mapper actor
        /// Called by: service code
        /// </summary>
        /// <param name="stringId"></param>
        /// <param name="targetPartitionInfo"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<ActorId> GetOrAddActorInPartition(string stringId, ServicePartitionInformation targetPartitionInfo, CancellationToken cancellationToken);

        /// <summary>
        /// Called on: Mapper actor
        /// Called by: user actors
        /// </summary>
        /// <param name="stringId"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<ActorId> GetExistingActor(string stringId, CancellationToken cancellationToken);
        #endregion

        #region LoadMetrics
        /// <summary>
        /// Called on: user actor
        /// Called by: user actor implementation
        /// </summary>
        /// <param name="load"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task ReportLoadMetric(int load, CancellationToken cancellationToken);

        /// <summary>
        /// Called on: MetricsReporter actor
        /// Called by: ReportLoadMetric() on user actor
        /// </summary>
        /// <param name="load"></param>
        /// <returns></returns>
        Task AggregateLoadMetric(int load);

        /// <summary>
        /// Called on: MetricsReporter actor
        /// Called by: service code
        /// </summary>
        /// <returns></returns>
        Task<int> GetAndClearAggregatedLoadMetric();
        #endregion

        #region Persistor
        //
        // General persistence methods
        //

        /// <summary>
        /// Called on: Persistor actor
        /// Called by: service code
        /// </summary>
        /// <param name="key"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<bool> IsPersisted(string key, CancellationToken cancellationToken);

        /// <summary>
        /// Called on: Persistor actor
        /// Called by: service code and Persistor actor
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<ConditionalValue<T>> TryGetState<T>(string key, CancellationToken cancellationToken);

        /// <summary>
        /// Called on: Persistor actor
        /// Called by: service code and Persistor actor
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task SetState<T>(string key, T value, CancellationToken cancellationToken);

        //
        // Checkpointing methods for use by user actor implementations
        //

        /// <summary>
        /// Called on: user actor
        /// Called by: user actor implementation
        /// </summary>
        /// <param name="e"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task CheckpointSingleEvent(EventData e, CancellationToken cancellationToken);

        /// <summary>
        /// Called on: user actor
        /// Called by: user actor implementation
        /// </summary>
        /// <param name="e"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task CheckpointAt(EventData e, CancellationToken cancellationToken);

        /// <summary>
        /// Called on: Persistor actor
        /// Called by: service code
        /// </summary>
        /// <param name="e"></param>
        /// <param name="sessionName"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task EventDispatched(EventData e, string sessionName, CancellationToken cancellationToken);

        /// <summary>
        /// Called on: Persistor actor
        /// Called by: Checkpoint* on user actor
        /// </summary>
        /// <param name="e"></param>
        /// <param name="andPreceding"></param>
        /// <param name="sessionName"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task MarkCompleted(EventData e, bool andPreceding, string sessionName, CancellationToken cancellationToken);

        /// <summary>
        /// Called on: Persistor actor
        /// Called by: service code
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<Dictionary<string, CompletionTracker>> GetAllCheckpoints(CancellationToken cancellationToken);
        #endregion
    }
}
