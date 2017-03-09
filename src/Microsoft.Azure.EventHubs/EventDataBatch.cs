// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using System.Collections.Generic;

    /// <summary>A helper class for creating a batch of EventData objects to be used for SendBatch or SendBatchAsync call.</summary>
    public class EventDataBatch
    {
        const int ReservedSize = 6 * 1024;
        const int MaxSizeLimit = 4 * 1024 * 1024;
        readonly List<EventData> eventDataList;
        long maxMessageSize;
        long currentSize;

        internal EventDataBatch(long maxMessageSize)
        {
            Fx.Assert(maxMessageSize > ReservedSize, "max message size too small");
            this.maxMessageSize = Math.Min(maxMessageSize, MaxSizeLimit);
            this.eventDataList = new List<EventData>();
            this.currentSize = ReservedSize;
        }

        /// <summary>Gets the current event count in the batch.</summary>
        public int Count
        {
            get
            {
                return this.eventDataList.Count;
            }
        }

        /// <summary>Tries to add an event data to the batch.</summary>
        /// <param name="eventData">The <see cref="Microsoft.ServiceBus.Messaging.EventData" /> to add.</param>
        /// <returns>A boolean value indicating if the event data has been added to the batch or not, based on the current size of the batch.</returns>
        /// <exception cref="InvalidOperationException">Thrown when an invalid EventData is being added.</exception>
        /// <remarks>Although this method name starts with a 'Try', it will throw Exceptions.</remarks>
        public bool TryAdd(EventData eventData)
        {
            if (eventData == null)
            {
                throw new InvalidOperationException(Resources.CannotSendAnEmptyEvent.FormatForUser(eventData.GetType().Name));
            }

            long size = GetSize(eventData, this.eventDataList.Count == 0);
            if (this.currentSize + size > this.maxMessageSize)
            {
                return false;
            }

            if (this.eventDataList.Count > 0)
            {
                Validate(this.eventDataList[0], eventData);
            }

            this.eventDataList.Add(eventData);
            this.currentSize += size;

            return true;
        }

        /// <summary>Converts the batch to an IEnumerable of EventData objects that can be accepted by the
        /// SendBatch or SendBatchAsync method.</summary>
        /// <returns>Returns an IEnumerable of EventData objects.</returns>
        public IEnumerable<EventData> ToEnumerable()
        {
            return this.eventDataList;
        }

        static void Validate(EventData first, EventData current)
        {
            if ((current.Body == null || current.Body.Count == 0)
                && current.SystemProperties == null
                && (current.Properties == null || current.Properties.Count == 0))
            {
                throw new InvalidOperationException(Resources.CannotSendAnEmptyEvent.FormatForUser(current.GetType().Name));
            }

            if (first.SystemProperties.PartitionKey != current.SystemProperties.PartitionKey)
            {
                throw new InvalidOperationException(
                    Resources.EventHubSendBatchMismatchPartitionKey.FormatForUser(
                        first.SystemProperties.PartitionKey ?? ClientConstants.NullString,
                        current.SystemProperties.PartitionKey ?? ClientConstants.NullString));
            }
        }

        static long GetSize(EventData eventData, bool first)
        {
            long size = 0;
//            long size = eventData.SerializedSizeInBytes;
//            size += 16; // Data section overhead
//            if (first)
//            {
//                int propSize = 0;
//                if (eventData.PartitionKey != null)
//                {
//                    propSize += SymbolEncoding.GetEncodeSize(AmqpMessageConverter.PartitionKeyName);
//                    propSize += StringEncoding.GetEncodeSize(eventData.PartitionKey);
//                }

//                if (eventData.Publisher != null)
//                {
//                    propSize += SymbolEncoding.GetEncodeSize(AmqpMessageConverter.PublisherName);
//                    propSize += StringEncoding.GetEncodeSize(eventData.Publisher);
//                }

//#if DEBUG
//                if (eventData.PartitionId != null)
//                {
//                    propSize += SymbolEncoding.GetEncodeSize(AmqpMessageConverter.PartitionIdName);
//                    propSize += ShortEncoding.GetEncodeSize(eventData.PartitionId);
//                }
//#endif

//                if (propSize > 0)
//                {
//                    size += propSize;
//                    size += 16; // message-annotation overhead
//                }
//            }

            return size;
        }
    }
}
