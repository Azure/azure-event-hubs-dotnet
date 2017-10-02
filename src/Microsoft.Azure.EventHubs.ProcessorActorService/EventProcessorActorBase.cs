using Microsoft.ServiceFabric.Actors.Runtime;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors;
using System.Fabric;
using System.Threading;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Actors.Client;
using System.Fabric.Description;

namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    /// <summary>
    /// Placeholder to avoid compiler error.
    /// </summary>
    public abstract class EventProcessorActorBase : Actor, IEventProcessorActor
    {
        /// <summary>
        /// Placeholder to avoid compiler error.
        /// </summary>
        public EventProcessorActorBase(ActorService actorService, ActorId actorId) : base(actorService, actorId)
        {
            this.aggregatedLoadMetric = 0;
        }

        #region Mapper
        /// <summary>
        /// Called on: Mapper actor
        /// Called by: service code
        /// If targetPartitionInfo is null, add in any partition.
        /// </summary>
        /// <param name="stringId"></param>
        /// <param name="targetPartitionInfo"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<ActorId> GetOrAddActorInPartition(string stringId, ServicePartitionInformation targetPartitionInfo, CancellationToken cancellationToken)
        {
            ConditionalValue<long> longId = await this.StateManager.TryGetStateAsync<long>(Constants.MapperActorMappingPrefix + stringId, cancellationToken);
            ActorId returnId = null;

            if (longId.HasValue)
            {
                // Get
                returnId = new ActorId(longId.Value);
            }
            else
            {
                // Add
                if (targetPartitionInfo != null)
                {
                    returnId = new ActorId(await AllocateIdInPartition((Int64RangePartitionInformation)targetPartitionInfo, cancellationToken));
                    await EstablishMapping(stringId, returnId.GetLongId(), cancellationToken);
                }
                else
                {
                    returnId = new ActorId(stringId);
                    await EstablishMapping(stringId, returnId.GetHashCode(), cancellationToken);
                }
            }

            return returnId;
        }

        /// <summary>
        /// Called on: Mapper actor
        /// Called by: user actors
        /// </summary>
        /// <param name="stringId"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<ActorId> GetExistingActor(string stringId, CancellationToken cancellationToken)
        {
            ConditionalValue<long> longId = await this.StateManager.TryGetStateAsync<long>(Constants.MapperActorMappingPrefix + stringId, cancellationToken);
            return longId.HasValue ? new ActorId(longId.Value) : null;
        }

        private Task<long> AllocateIdInPartition(Int64RangePartitionInformation targetPartitionInfo, CancellationToken cancellationToken)
        {
            string stateName = Constants.MapperNextAllocPrefix + targetPartitionInfo.Id.ToString();
            return this.StateManager.AddOrUpdateStateAsync<long>(stateName, targetPartitionInfo.LowKey, (key, oldval) => oldval + 1, cancellationToken);
        }

        private async Task EstablishMapping(string stringId, long longId, CancellationToken cancellationToken)
        {
            string stateName = Constants.MapperActorMappingPrefix + stringId;
            if (!await this.StateManager.TryAddStateAsync<long>(stateName, longId, cancellationToken))
            {
                long alreadyExisting = await this.StateManager.GetStateAsync<long>(stateName, cancellationToken);
                if (alreadyExisting != longId)
                {
                    // Name collision!
                    // TODO
                }
            }
        }
        #endregion

        #region LoadMetrics
        // The metric information is time-based and volatile. There is no point to persisting it across service restarts.
        // So it is just a member variable.
        private int aggregatedLoadMetric;

        private IUserActor cachedReportingActor = null;

        private async Task<IUserActor> GetReportingActor(CancellationToken cancellationToken)
        {
            if (this.cachedReportingActor == null)
            {
                this.cachedReportingActor = await GetWellKnownActorForPartition(Constants.MetricReporterActorIdPrefix, cancellationToken);
            }
            return this.cachedReportingActor;
        }

        /// <summary>
        /// Called on: user actor
        /// Called by: user actor implementation
        /// </summary>
        /// <param name="load"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task ReportLoadMetric(int load, CancellationToken cancellationToken)
        {
            IUserActor reportingActor = await GetReportingActor(cancellationToken);
            await this.cachedReportingActor.AggregateLoadMetric(load);
        }

        /// <summary>
        /// Called on: MetricsReporter actor
        /// Called by: ReportLoadMetric() on user actor
        /// Not a long-running call so no cancellationToken
        /// </summary>
        /// <param name="load"></param>
        /// <returns></returns>
        public Task AggregateLoadMetric(int load)
        {
            this.aggregatedLoadMetric += load;
            return Task.CompletedTask;
        }

        /// <summary>
        /// Called on: MetricsReporter actor
        /// Called by: service code
        /// Not a long-running call so no cancellationToken
        /// </summary>
        /// <returns></returns>
        public Task<int> GetAndClearAggregatedLoadMetric()
        {
            Task<int> result = Task.FromResult<int>(this.aggregatedLoadMetric);
            this.aggregatedLoadMetric = 0;
            return result;
        }
        #endregion

        #region Persistor
        /// <summary>
        /// Called on: Persistor actor
        /// Called by: service code
        /// </summary>
        /// <param name="key"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<bool> IsPersisted(string key, CancellationToken cancellationToken)
        {
            return this.StateManager.ContainsStateAsync(key, cancellationToken);
        }

        /// <summary>
        /// Called on: Persistor actor
        /// Called by: service code and Persistor actor
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<ConditionalValue<T>> TryGetState<T>(string key, CancellationToken cancellationToken)
        {
            return this.StateManager.TryGetStateAsync<T>(key, cancellationToken);
        }

        /// <summary>
        /// Called on: Persistor actor
        /// Called by: service code and Persistor actor
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task SetState<T>(string key, T value, CancellationToken cancellationToken)
        {
            return this.StateManager.SetStateAsync<T>(key, value, cancellationToken);
        }

        private Dictionary<string, CompletionTracker> cachedCheckpointMap = null;
        private bool? usingSessions = null;
        private string cachedSessionNameProperty = null;
        private IUserActor cachedPersistorActor = null;

        private async Task<IUserActor> GetPersistorActor(CancellationToken cancellationToken)
        {
            if (this.cachedPersistorActor == null)
            {
                this.cachedPersistorActor = await GetWellKnownActorForPartition(Constants.PersistorActorIdPrefix, cancellationToken);
            }
            return this.cachedPersistorActor;
        }

        private async Task GetCheckpointMap(CancellationToken cancellationToken)
        {
            if (this.cachedCheckpointMap == null)
            {
                ConditionalValue<Dictionary<string, CompletionTracker>> retrieved = await TryGetState<Dictionary<string, CompletionTracker>>(
                    Constants.CheckpointMapPrefix + this.ActorService.Context.PartitionId.ToString(), cancellationToken);
                if (retrieved.HasValue)
                {
                    this.cachedCheckpointMap = retrieved.Value;
                }
                else
                {
                    this.cachedCheckpointMap = new Dictionary<string, CompletionTracker>();
                }
            }
        }

        private async Task SaveCheckpointMap(CancellationToken cancellationToken)
        {
            if (this.cachedCheckpointMap == null)
            {
                throw new InvalidOperationException("Trying to persist null checkpoint map");
            }
            await SetState<Dictionary<string, CompletionTracker>>(Constants.CheckpointMapPrefix + this.ActorService.Context.PartitionId.ToString(),
                this.cachedCheckpointMap, cancellationToken);
        }

        private async Task<CompletionTracker> GetTracker(string sessionName, CancellationToken cancellationToken)
        {
            CompletionTracker result = null;
            await GetCheckpointMap(cancellationToken);
            if (this.cachedCheckpointMap.ContainsKey(sessionName))
            {
                result = this.cachedCheckpointMap[sessionName];
            }
            else
            {
                result = new CompletionTracker();
                this.cachedCheckpointMap.Add(sessionName, result);
                await SaveCheckpointMap(cancellationToken);
            }
            return result;
        }

        private string GetSessionName(EventData e)
        {
            if (!this.usingSessions.HasValue)
            {
                this.cachedSessionNameProperty = GetConfigurationValue(Constants.SessionNamePropertyName, null);
                this.usingSessions = this.cachedSessionNameProperty != null;
            }

            string result = Constants.NotUsingSessions;
            if (this.usingSessions.Value)
            {
                result = e.Properties.ContainsKey(this.cachedSessionNameProperty) ? e.Properties[this.cachedSessionNameProperty].ToString() : Constants.MissingSessionName;
            }
            return result;
        }

        /// <summary>
        /// Called on: user actor
        /// Called by: user actor implementation
        /// </summary>
        /// <param name="e"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task CheckpointSingleEvent(EventData e, CancellationToken cancellationToken)
        {
            IUserActor persistorActor = await GetPersistorActor(cancellationToken);
            string sessionName = GetSessionName(e);
            await this.cachedPersistorActor.MarkCompleted(e, false, sessionName, cancellationToken);
        }

        /// <summary>
        /// Called on: user actor
        /// Called by: user actor implementation
        /// </summary>
        /// <param name="e"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task CheckpointAt(EventData e, CancellationToken cancellationToken)
        {
            IUserActor persistorActor = await GetPersistorActor(cancellationToken);
            string sessionName = GetSessionName(e);
            await this.cachedPersistorActor.MarkCompleted(e, true, sessionName, cancellationToken);
        }

        /// <summary>
        /// Called on: Persistor actor
        /// Called by: service code
        /// </summary>
        /// <param name="e"></param>
        /// <param name="sessionName"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task EventDispatched(EventData e, string sessionName, CancellationToken cancellationToken)
        {
            await GetCheckpointMap(cancellationToken);
            CompletionTracker tracker = await GetTracker(sessionName, cancellationToken);
            tracker.AddEvent(e);
            await SaveCheckpointMap(cancellationToken);
        }

        /// <summary>
        /// Called on: Persistor actor
        /// Called by: Checkpoint* on user actor
        /// </summary>
        /// <param name="e"></param>
        /// <param name="andPreceding"></param>
        /// <param name="sessionName"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task MarkCompleted(EventData e, bool andPreceding, string sessionName, CancellationToken cancellationToken)
        {
            await GetCheckpointMap(cancellationToken);
            CompletionTracker tracker = await GetTracker(sessionName, cancellationToken);
            if (andPreceding)
            {
                tracker.CompleteEventAndPreceding(e);
            }
            else
            {
                tracker.CompleteEvent(e);
            }
            await SaveCheckpointMap(cancellationToken);
        }

        /// <summary>
        /// Called on: Persistor actor
        /// Called by: service code
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<Dictionary<string, CompletionTracker>> GetAllCheckpoints(CancellationToken cancellationToken)
        {
            await GetCheckpointMap(cancellationToken);
            // This is called by EventProcessorActorBase and ReceiveHandler on receiver startup. They do not want the live data,
            // they want a static copy of the state to determine the starting offset and filter out already-processed messages.
            // So return a copy.
            return new Dictionary<string, CompletionTracker>(this.cachedCheckpointMap);
        }
        #endregion

        #region Utilities
        private IUserActor cachedMapper = null;

        private IUserActor GetMapper()
        {
            if (this.cachedMapper == null)
            {
                this.cachedMapper = ActorProxy.Create<IUserActor>(Constants.MapperActorId, this.ActorService.Context.ServiceName);
            }
            return cachedMapper;
        }

        private async Task<IUserActor> GetWellKnownActorForPartition(string actorIdPrefix, CancellationToken cancellationToken)
        {
            // First, get the mapper.
            IUserActor mapper = GetMapper();
            // Now, ask the mapper for the id of the thing for this partition.
            string stringId = actorIdPrefix + this.ActorService.Context.PartitionId.ToString();
            ActorId id = await mapper.GetExistingActor(stringId, cancellationToken);
            // Finally, get proxy to the thing.
            return ActorProxy.Create<IUserActor>(id, EventProcessorActorService.ServiceName);
        }

        private string GetConfigurationValue(string configurationValueName, string defaultValue = null)
        {
            string value = defaultValue;
            ConfigurationPackage configPackage = this.ActorService.Context.CodePackageActivationContext.GetConfigurationPackageObject("Config");
            try
            {
                ConfigurationSection configSection = configPackage.Settings.Sections[Constants.EventProcessorConfigSectionName];
                ConfigurationProperty configProp = configSection.Parameters[configurationValueName];
            }
            catch (KeyNotFoundException)
            {
                // If the user has not specified a value in config, drop through and return the default value.
                // If the caller cannot continue without a value, it is up to the caller to detect and handle.
            }
            // catch ( ArgumentNullException e) if configurationValueName is null, that's a code bug, do not catch
            return value;
        }
        #endregion
    }
}
