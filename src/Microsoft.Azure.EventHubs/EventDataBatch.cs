// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.EventHubs.Amqp;

    /// <summary>A helper class for creating a batch of EventData objects to be used for SendBatch or SendBatchAsync call.</summary>
    public class EventDataBatch
    {
        const int MaxSizeLimit = 4 * 1024 * 1024;
        readonly List<EventData> eventDataList;
        long maxSize;
        long currentSize;

        public EventDataBatch(long maxSizeInBytes)
        {
            this.maxSize = Math.Min(maxSizeInBytes, MaxSizeLimit);
            this.eventDataList = new List<EventData>();
            this.currentSize = (maxSizeInBytes / 65536) * 1024;    // reserve 1KB for every 64KB
        }

        /// <summary>Gets the current event count in the batch.</summary>
        public int Count
        {
            get
            {
                return this.eventDataList.Count;
            }
        }

        /// <summary>Tries to add an event data to the batch if permitted by the batch's size limit.</summary>
        /// <param name="eventData">The <see cref="Microsoft.Azure.EventHubs.EventData" /> to add.</param>
        /// <returns>A boolean value indicating if the event data has been added to the batch or not.</returns>
        /// <exception cref="ArgumentNullException">Thrown when the EventData is null.</exception>
        /// <remarks>
        /// This method checks the sizes of the batch, the EventData object and the specified limit to determine
        /// if the EventData object can be added.
        /// </remarks>
        public bool TryAdd(EventData eventData)
        {
            if (eventData == null)
            {
                throw new ArgumentNullException(nameof(eventData));
            }

            long size = GetSize(eventData, this.eventDataList.Count == 0);
            if (this.currentSize + size > this.maxSize)
            {
                return false;
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

        static long GetSize(EventData eventData, bool first)
        {
            // Create AMQP message here. We will use the same message while sending to save compute time.
            eventData.AmqpMessage = AmqpMessageConverter.EventDataToAmqpMessage(eventData);

            return eventData.AmqpMessage.SerializedMessageSize;
        }
    }
}
