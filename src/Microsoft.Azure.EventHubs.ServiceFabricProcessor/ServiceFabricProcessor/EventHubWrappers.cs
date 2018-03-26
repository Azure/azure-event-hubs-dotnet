// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    public class EventHubWrappers
    {
        public interface IPartitionReceiveHandler2
        {
            Task ProcessEventsAsync(IEnumerable<IEventData> events);

            Task ProcessErrorAsync(Exception error);
        }

        public interface IPartitionReceiver
        {
            Task<IEnumerable<IEventData>> ReceiveAsync(int maxEventCount, TimeSpan waitTime);

            void SetReceiveHandler(IPartitionReceiveHandler2 receiveHandler, bool invokeWhenNoEvents = false);

            Task CloseAsync();
        }

        public interface ISystemPropertiesCollection
        {
            long SequenceNumber { get; }

            DateTime EnqueuedTimeUtc { get;  }

            string Offset { get; }

            string PartitionKey { get; }
        }

        public interface IEventData
        {
            ArraySegment<byte> Body { get; }

            IDictionary<string, object> Properties { get; }

            ISystemPropertiesCollection SystemProperties { get;  }

            void Dispose();
        }

        public interface IEventHubClient
        {
            Task<EventHubRuntimeInformation> GetRuntimeInformationAsync();

            IPartitionReceiver CreateEpochReceiver(string consumerGroupName, string partitionId, EventPosition eventPosition, long epoch, ReceiverOptions receiverOptions);

            Task CloseAsync();
        }

        public interface IEventHubClientFactory
        {
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

            public IPartitionReceiver CreateEpochReceiver(string consumerGroupName, string partitionId, EventPosition eventPosition, long epoch, ReceiverOptions receiverOptions)
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
