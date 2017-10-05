// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    using System.Diagnostics.Tracing;
    using System.Fabric;

    /// <summary>
    /// EventSource for Microsoft-Azure-EventHubs traces.
    /// 
    /// When defining Start/Stop tasks, the StopEvent.Id must be exactly StartEvent.Id + 1.
    /// 
    /// Do not explicity include the Guid here, since EventSource has a mechanism to automatically
    /// map to an EventSource Guid based on the Name (Microsoft-Azure-EventHubs).
    /// </summary>
    [EventSource(Name = "Microsoft-Azure-EventProcessorActorService")]
    internal sealed class EventProcessorActorServiceEventSource : EventSource
    {
        public static EventProcessorActorServiceEventSource Log { get; } = new EventProcessorActorServiceEventSource();

        EventProcessorActorServiceEventSource()
        { }

        [Event(1, Level = EventLevel.Informational, Message = "Partition {0} replica {1} opening")]
        public void BeginOpenReplica(StatefulServiceContext context)
        {
            if (IsEnabled())
            {
                WriteEvent(1, context.PartitionId, context.ReplicaId);
            }
        }

        [Event(2, Level = EventLevel.Informational, Message = "Partition {0} replica {1} opened")]
        public void EndOpenReplica(StatefulServiceContext context)
        {
            if (IsEnabled())
            {
                WriteEvent(2, context.PartitionId, context.ReplicaId);
            }
        }

        [Event(3, Level = EventLevel.Informational, Message = "Partition {0} replica {1} has ordinal {2} of {3} service fabric partitions")]
        public void ReplicaOrdinals(StatefulServiceContext context, int ordinal, int partitions)
        {
            if (IsEnabled())
            {
                WriteEvent(3, context.PartitionId, context.ReplicaId, ordinal, partitions);
            }
        }

        [Event(4, Level = EventLevel.Informational, Message = "Partition {0} replica {1} closing")]
        public void BeginCloseReplica(StatefulServiceContext context)
        {
            if (IsEnabled())
            {
                WriteEvent(4, context.PartitionId, context.ReplicaId);
            }
        }

        [Event(5, Level = EventLevel.Informational, Message = "Partition {0} replica {1} closed")]
        public void EndCloseReplica(StatefulServiceContext context)
        {
            if (IsEnabled())
            {
                WriteEvent(5, context.PartitionId, context.ReplicaId);
            }
        }

        [Event(6, Level = EventLevel.Informational, Message = "Partition {0} replica {1} changing role from {2} to {3}")]
        public void ReplicaChangeRole(StatefulServiceContext context, string oldRole, string newRole)
        {
            if (IsEnabled())
            {
                WriteEvent(6, context.PartitionId, context.ReplicaId, oldRole, newRole);
            }
        }

        [Event(7, Level = EventLevel.Informational, Message = "Partition {0} replica {1} setting metrics description")]
        public void SettingMetricsDescription(StatefulServiceContext context)
        {
            if (IsEnabled())
            {
                WriteEvent(7, context.PartitionId, context.ReplicaId);
            }
        }

        [Event(8, Level = EventLevel.Informational, Message = "Partition {0} replica {1} starting receiver at offset {2}")]
        public void StartReceiver(StatefulServiceContext context, string offset)
        {
            if (IsEnabled())
            {
                WriteEvent(8, context.PartitionId, context.ReplicaId, offset);
            }
        }

        [Event(9, Level = EventLevel.Informational, Message = "Partition {0} replica {1} stopping receiver")]
        public void StopReceiver(StatefulServiceContext context)
        {
            if (IsEnabled())
            {
                WriteEvent(9, context.PartitionId, context.ReplicaId);
            }
        }

        [Event(10, Level = EventLevel.Verbose, Message = "Partition {0} replica {1} reporting load {2}")]
        public void LoadReport(StatefulServiceContext context, int load)
        {
            if (IsEnabled())
            {
                WriteEvent(10, context.PartitionId, context.ReplicaId, load);
            }
        }

        [Event(11, Level = EventLevel.Warning, Message = "Partition {0} replica {1} max concurrent calls bad setting: {2}")]
        public void MaxConcurrentCallsSettingError(StatefulServiceContext context, string error)
        {
            if (IsEnabled())
            {
                WriteEvent(11, context.PartitionId, context.ReplicaId, error);
            }
        }

        [Event(12, Level = EventLevel.Informational, Message = "Partition {0} replica {1} start receive handler max concurrent {0} session property {1}")]
        public void StartReceiveHandler(StatefulServiceContext context, int maxConcurrent, string sessionProperty)
        {
            if (IsEnabled())
            {
                WriteEvent(12, context.PartitionId, context.ReplicaId, maxConcurrent, sessionProperty ?? "none");
            }
        }

        [Event(13, Level = EventLevel.Verbose, Message = "Partition {0} replica {1} dispatching events to {3}")]
        public void DispatchingEvents(StatefulServiceContext context, string actorId)
        {
            if (IsEnabled())
            {
                WriteEvent(13, context.PartitionId, context.ReplicaId, actorId);
            }
        }

        [Event(14, Level = EventLevel.Verbose, Message = "Establish actor mapping {0} to {1}")]
        public void EstablishActorMaping(string stringId, long longId)
        {
            if (IsEnabled())
            {
                WriteEvent(14, stringId, longId);
            }
        }

        [Event(15, Level = EventLevel.Verbose, Message = "Checkpointing {0} {1}//{2}")]
        public void Checkpointing(bool single, string offset, long sequenceNumber)
        {
            if (IsEnabled())
            {
                WriteEvent(15, single ? "single event" : "up to and including", offset, sequenceNumber);
            }
        }
    }
}
