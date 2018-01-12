// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using Microsoft.Azure.Amqp;
    using Microsoft.Azure.EventHubs.Amqp;

    /// <summary>
    /// Represents options can be set during the creation of a event hub receiver.
    /// </summary> 
    /// <summary>
    /// Defines a position of an <see cref="EventData" /> in the event hub partition.
    /// The position can be one of <see cref="EventData.SystemPropertiesCollection.Offset"/>, <see cref="EventData.SystemPropertiesCollection.SequenceNumber"/>
    /// or <see cref="EventData.SystemPropertiesCollection.EnqueuedTimeUtc"/>.
    /// </summary>
    public class EventPosition
    {
        EventPosition() { }

        /// <summary>
        /// Creates a position at the given offset.
        /// </summary>
        /// <param name="offset"><see cref="EventData.SystemPropertiesCollection.Offset"/> </param>
        /// <param name="inclusive">If true, the specified event is included; otherwise the next event is returned.</param>
        /// <returns>An <see cref="EventPosition"/> object.</returns>
        public static EventPosition FromOffset(string offset, bool inclusive = false)
        {
            if (string.IsNullOrEmpty(offset))
            {
                throw new ArgumentNullException(nameof(offset));
            }

            return new EventPosition() { Offset = offset, OffsetInclusive = inclusive };
        }

        /// <summary>
        /// Creates a position at the given offset.
        /// </summary>
        /// <param name="sequenceNumber"><see cref="EventData.SystemPropertiesCollection.SequenceNumber"/></param>
        /// <returns>An <see cref="EventPosition"/> object.</returns>
        public static EventPosition FromSequenceNumber(long sequenceNumber)
        {
            return new EventPosition() { SequenceNumber = sequenceNumber };
        }

        /// <summary>
        /// Creates a position at the given offset.
        /// </summary>
        /// <param name="enqueuedTimeUtc"><see cref="EventData.SystemPropertiesCollection.EnqueuedTimeUtc"/></param>
        /// <returns>An <see cref="EventPosition"/> object.</returns>
        public static EventPosition FromEnqueuedTime(DateTime enqueuedTimeUtc)
        {
            return new EventPosition() { EnqueuedTimeUtc = enqueuedTimeUtc };
        }

        /// <summary>
        /// Gets the offset of the event at the position. It can be null if the position is just created
        /// from a sequence number or an enqueued time.
        /// </summary>
        public string Offset
        {
            get;
            internal set;
        }

        /// <summary>
        /// Indicates if the current event at the specified offset is included or not.
        /// It is only applicable if offset is set.
        /// </summary>
        public bool OffsetInclusive
        {
            get;
            internal set;
        }

        /// <summary>
        /// Gets the enqueued time of the event at the position. It can be null if the position is just created
        /// from an offset or a sequence number.
        /// </summary>
        public DateTime? EnqueuedTimeUtc
        {
            get;
            internal set;
        }

        /// <summary>
        /// Gets the sequence number of the event at the position. It can be null if the position is just created
        /// from an offset or an enqueued time.
        /// </summary>
        public long? SequenceNumber
        {
            get;
            internal set;
        }

        internal string GetExpression()
        {
            // order of preference
            if (this.Offset != null)
            {
                return this.OffsetInclusive ?
                    $"{AmqpClientConstants.FilterOffsetPartName} >= {this.Offset}" :
                    $"{AmqpClientConstants.FilterOffsetPartName} > {this.Offset}";
            }

            if (this.SequenceNumber.HasValue)
            {
                return $"{AmqpClientConstants.FilterSeqNumberName} > {this.SequenceNumber.Value}";
            }

            if (this.EnqueuedTimeUtc.HasValue)
            {
                long ms = TimeStampEncodingGetMilliseconds(this.EnqueuedTimeUtc.Value);
                return $"{AmqpClientConstants.FilterReceivedAtPartNameV2} > {ms}";
            }

            throw new ArgumentException("No starting position was set");
        }


        // This is equivalent to Microsoft.Azure.Amqp's internal API TimeStampEncoding.GetMilliseconds
        long TimeStampEncodingGetMilliseconds(DateTime value)
        {
            DateTime utcValue = value.ToUniversalTime();
            double milliseconds = (utcValue - AmqpConstants.StartOfEpoch).TotalMilliseconds;
            return (long)milliseconds;
        }
    }
}
