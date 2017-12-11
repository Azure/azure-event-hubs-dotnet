// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.EventHubs.Amqp;

    /// <summary>A helper class for creating an IEnumerable&lt;<see cref="Microsoft.Azure.EventHubs.EventData"/>&gt; taking into account the max size limit, so that the IEnumerable&lt;<see cref="Microsoft.Azure.EventHubs.EventData"/>&gt; can be passed to the Send or SendAsync method of an EventHubClient to send the EventData objects as a batch.</summary>
    public class EventDataBatch : IDisposable
    {
        const int MaxSizeLimit = 4 * 1024 * 1024;
        readonly List<EventData> eventDataList;
        long maxSize;
        long currentSize;
        bool disposed;

        /// <summary>
        /// Creates a new <see cref="EventDataBatch"/>.
        /// </summary>
        /// <param name="maxSizeInBytes">The maximum size allowed for the batch</param>
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
                this.ThrowIfDisposed();
                return this.eventDataList.Count;
            }
        }

        /// <summary>Tries to add an event data to the batch if permitted by the batch's size limit.</summary>
        /// <param name="eventData">The <see cref="Microsoft.Azure.EventHubs.EventData" /> to add.</param>
        /// <returns>A boolean value indicating if the event data has been added to the batch or not.</returns>
        /// <exception cref="ArgumentNullException">Thrown when the EventData is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the batch is already disposed.</exception>
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

            this.ThrowIfDisposed();
            long size = GetSize(eventData);
            if (this.currentSize + size > this.maxSize)
            {
                return false;
            }

            this.eventDataList.Add(eventData);
            this.currentSize += size;

            return true;
        }

        /// <summary>Converts the batch to an IEnumerable of EventData objects that can be accepted by the
        /// SendBatchAsync method.</summary>
        /// <returns>Returns an IEnumerable of EventData objects.</returns>
        public IEnumerable<EventData> ToEnumerable()
        {
            this.ThrowIfDisposed();
            return this.eventDataList;
        }

        static long GetSize(EventData eventData)
        {
            // Create AMQP message here. We will use the same message while sending to save compute time.
            eventData.AmqpMessage = AmqpMessageConverter.EventDataToAmqpMessage(eventData);

            return eventData.AmqpMessage.SerializedMessageSize;
        }

        /// <summary>
        /// Disposes resources attached to an EventDataBatch.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    this.disposed = true;
                    foreach (var e in this.eventDataList)
                    {
                        e.Dispose();
                    }
                }

                disposed = true;
            }
        }

        void ThrowIfDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }
        }
   }
}
