using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Actors.Client;
using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    class ReceiveHandler : IPartitionReceiveHandler
    {
        private EventProcessorActorService parent;
        private CancellationToken cancellationToken;

        private int maxConcurrentActorCalls;
        private int roundRobinScanner = 0;
        private string[] roundRobinStringIds;
        private string sessionNameProperty;

        private ActorThrottle throttle;

        private Dictionary<string, CompletionTracker> startingCheckpoints;

        internal ReceiveHandler(EventProcessorActorService parent, Dictionary<string, CompletionTracker> startingCheckpoints, CancellationToken cancellationToken)
        {
            this.parent = parent;
            this.startingCheckpoints = startingCheckpoints;
            this.cancellationToken = cancellationToken;

            string maxCallsString = ServiceUtilities.GetConfigurationValue(Constants.MaxConcurrentActorCallsName, Constants.MaxConcurrentActorCallsDefault.ToString());
            try
            {
                this.maxConcurrentActorCalls = int.Parse(maxCallsString);
            }
            catch (ArgumentNullException ane)
            {
                // TODO trace
                this.maxConcurrentActorCalls = Constants.MaxConcurrentActorCallsDefault;
            }
            catch (FormatException fe)
            {
                // TODO trace
                this.maxConcurrentActorCalls = Constants.MaxConcurrentActorCallsDefault;
            }
            catch (OverflowException oe)
            {
                // TODO trace
                this.maxConcurrentActorCalls = Constants.MaxConcurrentActorCallsDefault;
            }

            this.sessionNameProperty = ServiceUtilities.GetConfigurationValue(Constants.SessionNamePropertyName, Constants.SessionNamePropertyDefault);

            if ((this.sessionNameProperty == null) && (this.maxConcurrentActorCalls > 1))
            {
                GetOrGenerateActorIdSet();
            }

            this.throttle = new ActorThrottle(this.maxConcurrentActorCalls);
        }

        public int MaxBatchSize
        {
            get
            {
                return Constants.DefaultMaxBatchSize;
            }
        }

        public Task ProcessErrorAsync(Exception error)
        {
            throw new NotImplementedException(); // TODO
        }

        public async Task ProcessEventsAsync(IEnumerable<EventData> events)
        {
            List<Tuple<ActorId, IEnumerable<EventData>>> batches = await FilterAndRebatchWithActorIds(events);
            foreach (Tuple<ActorId, IEnumerable<EventData>> batch in batches)
            {
                int slot = this.throttle.GetAvailableSlot();
                IUserActor dispatchTo = ActorProxy.Create<IUserActor>(batch.Item1, EventProcessorActorService.ServiceName);
                Task pending = dispatchTo.OnReceive(batch.Item2, this.cancellationToken);
                this.throttle.AddPendingCall(pending, slot);
            }
            await this.throttle.WaitForFinish(this.cancellationToken);
         }

        private void GetOrGenerateActorIdSet()
        {
            this.roundRobinStringIds = new string[this.maxConcurrentActorCalls];
            string stateId = Constants.PartitionRoundRobinActorsPrefix + this.parent.PartitionInfo.Id.ToString();

            IUserActor persistor = ServiceUtilities.GetPersistorForPartition(this.parent.PartitionInfo, this.cancellationToken).Result;
            // Try to get actor ids from the state store.
            ConditionalValue<string[]> ids = persistor.TryGetState<string[]>(stateId, this.cancellationToken).Result;
            if (ids.HasValue)
            {
                this.roundRobinStringIds = ids.Value;
            }
            else
            {
                // First time service is run, generate a new set of ids and store.
                for (int i = 0; i < this.roundRobinStringIds.Length; i++)
                {
                    this.roundRobinStringIds[i] = Constants.RoundRobinActorIdPrefix + Guid.NewGuid().ToString();
                }
                persistor.SetState<string[]>(stateId, this.roundRobinStringIds, this.cancellationToken).Wait();
            }
        }

        private async Task<List<Tuple<ActorId, IEnumerable<EventData>>>> FilterAndRebatchWithActorIds(IEnumerable<EventData> events)
        {
            List<Tuple<ActorId, IEnumerable<EventData>>> result = new List<Tuple<ActorId, IEnumerable<EventData>>>();
            ActorId dispatchTo;

            if (this.sessionNameProperty != null)
            {
                // Divide into batches according to value of session name property
                // Actor id is the session name
                Dictionary<string, List<EventData>> perSessionLists = new Dictionary<string, List<EventData>>();
                foreach (EventData e in events)
                {
                    string sessionName = GetSessionName(e);
                    if (DoAddToList(e, sessionName))
                    {
                        if (perSessionLists.ContainsKey(sessionName))
                        {
                            perSessionLists[sessionName].Add(e);
                        }
                        else
                        {
                            perSessionLists.Add(sessionName, new List<EventData>() { e });
                        }
                    }
                }
                foreach (KeyValuePair<string, List<EventData>> kvp in perSessionLists)
                {
                    dispatchTo = await DetermineDispatchTarget(kvp.Key);
                    result.Add(new Tuple<ActorId, IEnumerable<EventData>>(dispatchTo, kvp.Value));
                }
            }
            else if (this.maxConcurrentActorCalls == 1)
            {
                // Entire batch goes to an actor named after the partition
                dispatchTo = await DetermineDispatchTarget(this.parent.PartitionOrdinal.ToString());
                if (this.startingCheckpoints.Count == 0)
                {
                    // Fast forward
                    result.Add(new Tuple<ActorId, IEnumerable<EventData>>(dispatchTo, events));
                }
                else
                {
                    // Filter events against checkpoints
                    List<EventData> filteredEvents = new List<EventData>();
                    foreach (EventData ev in events)
                    {
                        if (DoAddToList(ev, Constants.NotUsingSessions))
                        {
                            filteredEvents.Add(ev);
                        }
                    }
                    result.Add(new Tuple<ActorId, IEnumerable<EventData>>(dispatchTo, filteredEvents));
                }
            }
            else
            {
                // Entire batch goes to one of a fixed set of actors, round-robin
                dispatchTo = await DetermineDispatchTarget(this.roundRobinStringIds[this.roundRobinScanner]);
                this.roundRobinScanner = (this.roundRobinScanner + 1) % this.roundRobinStringIds.Length;
                if (this.startingCheckpoints.Count == 0)
                {
                    // Fast forward
                    result.Add(new Tuple<ActorId, IEnumerable<EventData>>(dispatchTo, events));
                }
                else
                {
                    // Filter events against checkpoints
                    List<EventData> filteredEvents = new List<EventData>();
                    foreach (EventData ev in events)
                    {
                        if (DoAddToList(ev, Constants.NotUsingSessions))
                        {
                            filteredEvents.Add(ev);
                        }
                    }
                    result.Add(new Tuple<ActorId, IEnumerable<EventData>>(dispatchTo, filteredEvents));
                }
            }

            return result;
        }

        private bool DoAddToList(EventData e, string sessionName)
        {
            bool result = true;

            if (this.startingCheckpoints.ContainsKey(sessionName))
            {
                CompletionTracker tracker =  this.startingCheckpoints[sessionName];
                if (tracker.IsPastHighest(e))
                {
                    // Past the end of the map for this session, so remove it so that we can short-circuit to true in the future.
                    this.startingCheckpoints.Remove(sessionName);
                }
                else
                {
                    result = !tracker.IsCompleted(e);
                }
            }

            return result;
        }

        private string GetSessionName(EventData e)
        {
            return e.Properties.ContainsKey(this.sessionNameProperty) ? e.Properties[this.sessionNameProperty].ToString() : Constants.MissingSessionName;
        }

        private async Task<ActorId> DetermineDispatchTarget(string stringId)
        {
            ActorId result = null;

            if (this.parent.NonReceivingPartitionsPresent)
            {
                // There are more service partitions than event hub partitions. In order to allow scale out
                // by increasing the number of service partitions, we must not require that actors are in the
                // same partition as the receiver. So let the regular Service Fabric hashing place actors wherever
                // based on the string.
                result = await ServiceUtilities.Mapper.GetOrAddActorInPartition(stringId, null, this.cancellationToken);
            }
            else
            {
                result = await ServiceUtilities.Mapper.GetOrAddActorInPartition(stringId, this.parent.PartitionInfo, this.cancellationToken);
            }

            return result;
        }
    }
}
