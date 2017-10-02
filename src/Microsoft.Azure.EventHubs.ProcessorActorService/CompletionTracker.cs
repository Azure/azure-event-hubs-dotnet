using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;

namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    /// <summary>
    /// Placeholder to avoid compiler error.
    /// </summary>
    [DataContract]
    public class CompletionTracker
    {
        private Queue<EventTracker> trackers;
        private string highestOffset;
        private long highestSequenceNumber;

        internal CompletionTracker()
        {
            this.trackers = new Queue<EventTracker>();
            this.highestOffset = PartitionReceiver.StartOfStream;
            this.highestSequenceNumber = -1;
        }

        internal void AddEvent(EventData e)
        {
            this.trackers.Enqueue(new EventTracker(e.SystemProperties.Offset, e.SystemProperties.SequenceNumber));
            this.highestOffset = e.SystemProperties.Offset;
        }

        internal void CompleteEvent(EventData e)
        {
            EventTracker completeThis = FindEvent(e);
            if (completeThis != null)
            {
                completeThis.IsCompleted = true;
            }
            else
            {
                throw new InvalidOperationException("No tracker found for event");
            }

            TrimCompleted();
        }

        internal void CompleteEventAndPreceding(EventData e)
        {
            if (FindEvent(e) == null)
            {
                throw new InvalidOperationException("No tracker found for event");
            }
            foreach (EventTracker et in this.trackers)
            {
                et.IsCompleted = true;
                if (et.SequenceNumber == e.SystemProperties.SequenceNumber)
                {
                    break;
                }
            }

            TrimCompleted();
        }

        private EventTracker FindEvent(EventData e)
        {
            EventTracker result = null;
            foreach (EventTracker et in this.trackers)
            {
                if (et.SequenceNumber == e.SystemProperties.SequenceNumber)
                {
                    result = et;
                    break;
                }
            }
            return result;
        }

        private void TrimCompleted()
        {
            // Trim all completed events from the start of the queue.
            try
            {
                while (this.trackers.Peek().IsCompleted)
                {
                    this.trackers.Dequeue();
                }
            }
            catch (InvalidOperationException)
            {
                // this.trackers is empty, which is fine.
            }
        }

        internal bool IsPastHighest(EventData e)
        {
            return (e.SystemProperties.SequenceNumber > this.highestSequenceNumber);
        }

        internal Tuple<string, long> GetLowestUncompletedOffsetAndSequenceNumber()
        {
            Tuple<string, long> result = new Tuple<string, long>(this.highestOffset, this.highestSequenceNumber);
            foreach (EventTracker et in this.trackers)
            {
                if (!et.IsCompleted)
                {
                    result = new Tuple<string, long>(et.Offset, et.SequenceNumber);
                    break;
                }
            }
            return result;
        }

        internal bool IsCompleted(EventData e)
        {
            bool result = false; ;

            if (IsPastHighest(e))
            {
                // Events after this.highestOffset are not completed.
                result = false;
            }
            else if (this.trackers.Count == 0)
            {
                // Nothing in this.trackers, so everything before or including this.highestOffset is completed.
                result = true;
            }
            else if (e.SystemProperties.SequenceNumber < this.trackers.Peek().SequenceNumber) // Peek is safe because we know tracker is not empty
            {
                // Events before the first in this.trackers are completed.
                result = true;
            }
            else
            {
                // Scan this.trackers for e and return the state.
                bool found = false;
                foreach (EventTracker et in this.trackers)
                {
                    if (et.Offset == e.SystemProperties.Offset)
                    {
                        found = true;
                        result = et.IsCompleted;
                        break;
                    }
                }
                if (!found)
                {
                    throw new InvalidOperationException("No tracker found for event");
                }
            }

            return result;
        }

        [DataContract]
        private class EventTracker
        {
            internal EventTracker(string offset, long sequenceNumber)
            {
                this.Offset = offset;
                this.SequenceNumber = sequenceNumber;
                this.IsCompleted = false;
            }

            internal string Offset { get; private set; }
            internal long SequenceNumber { get; private set; }
            internal bool IsCompleted { get; set; }
        }
    }
}
