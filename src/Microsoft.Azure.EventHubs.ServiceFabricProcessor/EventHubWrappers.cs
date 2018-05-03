// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    /// <summary>
    /// Wrappers for the underlying Event Hub client which allow mocking.
    /// The interfaces include only the client functionality used by the Service Fabric Processor.
    /// </summary>
    public class EventHubWrappers
    {
        /// <summary>
        /// Interface for a receive handler.
        /// </summary>
        public interface IPartitionReceiveHandler2
        {
            /// <summary>
            /// </summary>
            /// <param name="events"></param>
            /// <returns></returns>
            Task ProcessEventsAsync(IEnumerable<IEventData> events);

            /// <summary>
            /// </summary>
            /// <param name="error"></param>
            /// <returns></returns>
            Task ProcessErrorAsync(Exception error);
        }

        /// <summary>
        /// Interface for a partition receiver.
        /// </summary>
        public interface IPartitionReceiver
        {
            /// <summary>
            /// </summary>
            /// <param name="maxEventCount"></param>
            /// <param name="waitTime"></param>
            /// <returns></returns>
            Task<IEnumerable<IEventData>> ReceiveAsync(int maxEventCount, TimeSpan waitTime);

            /// <summary>
            /// </summary>
            /// <param name="receiveHandler"></param>
            /// <param name="invokeWhenNoEvents"></param>
            void SetReceiveHandler(IPartitionReceiveHandler2 receiveHandler, bool invokeWhenNoEvents = false);

            /// <summary>
            /// </summary>
            /// <returns></returns>
            Task CloseAsync();
        }

        /// <summary>
        /// Interface representing system properties on an event.
        /// </summary>
        public interface ISystemPropertiesCollection
        {
            /// <summary>
            /// </summary>
            long SequenceNumber { get; }

            /// <summary>
            /// </summary>
            DateTime EnqueuedTimeUtc { get; }

            /// <summary>
            /// </summary>
            string Offset { get; }

            /// <summary>
            /// </summary>
            string PartitionKey { get; }
        }

        /// <summary>
        /// Interface representing an event.
        /// </summary>
        public interface IEventData
        {
            /// <summary>
            /// </summary>
            ArraySegment<byte> Body { get; }

            /// <summary>
            /// </summary>
            IDictionary<string, object> Properties { get; }

            /// <summary>
            /// </summary>
            ISystemPropertiesCollection SystemProperties { get; }

            /// <summary>
            /// </summary>
            void Dispose();
        }

        /// <summary>
        /// Interface representing EVentHubClient
        /// </summary>
        public interface IEventHubClient
        {
            /// <summary>
            /// </summary>
            /// <returns></returns>
            Task<EventHubRuntimeInformation> GetRuntimeInformationAsync();

            /// <summary>
            /// </summary>
            /// <param name="consumerGroupName"></param>
            /// <param name="partitionId"></param>
            /// <param name="eventPosition"></param>
            /// <param name="offset">Only used by mocks</param>
            /// <param name="epoch"></param>
            /// <param name="receiverOptions"></param>
            /// <returns></returns>
            IPartitionReceiver CreateEpochReceiver(string consumerGroupName, string partitionId, EventPosition eventPosition, string offset, long epoch, ReceiverOptions receiverOptions);

            /// <summary>
            /// </summary>
            /// <returns></returns>
            Task CloseAsync();
        }

        /// <summary>
        /// Interface for an EventHubClient factory so that we can have factories which dispense different implementations of IEventHubClient.
        /// </summary>
        public interface IEventHubClientFactory
        {
            /// <summary>
            /// </summary>
            /// <param name="connectionString"></param>
            /// <returns></returns>
            IEventHubClient CreateFromConnectionString(string connectionString);
        }

        internal class SystemPropertiesCollectionWrapper : ISystemPropertiesCollection
        {
            private readonly EventData.SystemPropertiesCollection inner;

            internal SystemPropertiesCollectionWrapper(EventData.SystemPropertiesCollection spc)
            {
                this.inner = spc;
            }

            public long SequenceNumber
            {
                get
                {
                    return this.inner.SequenceNumber;
                }
            }

            public DateTime EnqueuedTimeUtc
            {
                get
                {
                    return this.inner.EnqueuedTimeUtc;
                }
            }

            public string Offset
            {
                get
                {
                    return this.inner.Offset;
                }
            }

            public string PartitionKey
            {
                get
                {
                    return this.inner.PartitionKey;
                }
            }
        }

        internal class EventDataWrapper : IEventData
        {
            private readonly EventData inner;

            internal EventDataWrapper(EventData eventData)
            {
                this.inner = eventData;
            }

            public ArraySegment<byte> Body
            {
                get
                {
                    return this.inner.Body;
                }
            }

            public IDictionary<string, object> Properties
            {
                get
                {
                    return this.inner.Properties;
                }

            }

            public ISystemPropertiesCollection SystemProperties
            {
                get
                {
                    return new SystemPropertiesCollectionWrapper(this.inner.SystemProperties);
                }
            }

            public void Dispose()
            {
                this.inner.Dispose();
            }
        }

        internal class PartitionReceiverWrapper : IPartitionReceiver, IPartitionReceiveHandler
        {
            private readonly PartitionReceiver inner;
            private IPartitionReceiveHandler2 outerHandler = null;

            internal PartitionReceiverWrapper(PartitionReceiver receiver)
            {
                this.inner = receiver;
                this.MaxBatchSize = 10; // TODO get this from somewhere real
            }

            public async Task<IEnumerable<IEventData>> ReceiveAsync(int maxEventCount, TimeSpan waitTime)
            {
                IEnumerable<EventData> rawEvents = await this.inner.ReceiveAsync(maxEventCount, waitTime);
                IEnumerable<IEventData> wrappedEvents = null;
                if (rawEvents != null)
                {
                    wrappedEvents = WrapRawEvents(rawEvents);
                }
                return wrappedEvents;
            }

            public void SetReceiveHandler(IPartitionReceiveHandler2 receiveHandler, bool invokeWhenNoEvents = false)
            {
                this.outerHandler = receiveHandler;
                this.inner.SetReceiveHandler(this, invokeWhenNoEvents);
            }

            public Task CloseAsync()
            {
                return this.inner.CloseAsync();
            }

            public int MaxBatchSize { get; set; }

            public Task ProcessEventsAsync(IEnumerable<EventData> rawEvents)
            {
                IEnumerable<IEventData> wrappedEvents = (rawEvents != null) ? WrapRawEvents(rawEvents) : new List<IEventData>();
                return outerHandler.ProcessEventsAsync(wrappedEvents);
            }

            public Task ProcessErrorAsync(Exception error)
            {
                return outerHandler.ProcessErrorAsync(error);
            }

            IEnumerable<IEventData> WrapRawEvents(IEnumerable<EventData> rawEvents)
            {
                List<IEventData> wrappedEvents = new List<IEventData>();
                IEnumerator<EventData> rawScanner = rawEvents.GetEnumerator();
                while (rawScanner.MoveNext())
                {
                    wrappedEvents.Add(new EventDataWrapper(rawScanner.Current));
                }
                return wrappedEvents;
            }
        }

        internal class EventHubClientWrapper : IEventHubClient
        {
            private readonly EventHubClient inner;

            internal EventHubClientWrapper(EventHubClient ehc)
            {
                this.inner = ehc;
            }

            public Task<EventHubRuntimeInformation> GetRuntimeInformationAsync()
            {
                return this.inner.GetRuntimeInformationAsync();
            }

            public IPartitionReceiver CreateEpochReceiver(string consumerGroupName, string partitionId, EventPosition eventPosition, string offset, long epoch, ReceiverOptions receiverOptions)
            {
                return new PartitionReceiverWrapper(this.inner.CreateEpochReceiver(consumerGroupName, partitionId, eventPosition, epoch, receiverOptions));
            }

            public Task CloseAsync()
            {
                return this.inner.CloseAsync();
            }
        }

        internal class EventHubClientFactory : IEventHubClientFactory
        {
            public IEventHubClient CreateFromConnectionString(string connectionString)
            {
                return new EventHubClientWrapper(EventHubClient.CreateFromConnectionString(connectionString));
            }
        }
    }
}
