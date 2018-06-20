// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    /// <summary>
    /// Mocks for the underlying event hub client. Using these instead of the regular wrappers allows unit testing without an event hub.
    /// By default, EventProcessorService.EventHubClientFactory is a EventHubWrappers.EventHubClientFactory.
    /// To use the mocks, change it to a EventHubMocks.EventHubClientFactoryMock.
    /// </summary>
    public class EventHubMocks
    {
        /// <summary>
        /// Mock for system properties on an event.
        /// </summary>
        public class SystemPropertiesCollectionMock : EventHubWrappers.ISystemPropertiesCollection
        {
            /// <summary>
            /// Construct the mock system properties.
            /// </summary>
            /// <param name="sequenceNumber"></param>
            /// <param name="enqueuedTimeUtc"></param>
            /// <param name="offset"></param>
            /// <param name="partitionKey"></param>
            public SystemPropertiesCollectionMock(long sequenceNumber, DateTime enqueuedTimeUtc, string offset, string partitionKey)
            {
                this.SequenceNumber = sequenceNumber;
                this.EnqueuedTimeUtc = enqueuedTimeUtc;
                this.Offset = offset;
                this.PartitionKey = partitionKey;
            }

            /// <summary>
            /// Sequence number of the mock event.
            /// </summary>
            public long SequenceNumber { get; private set; }

            /// <summary>
            /// Enqueued time of the mock event.
            /// </summary>
            public DateTime EnqueuedTimeUtc { get; private set; }

            /// <summary>
            /// Offset of the mock event.
            /// </summary>
            public string Offset { get; private set; }

            /// <summary>
            /// Partition key of the mock event.
            /// </summary>
            public string PartitionKey { get; private set; }
        }

        /// <summary>
        /// Mock for an event.
        /// </summary>
        public class EventDataMock : EventHubWrappers.IEventData
        {
            /// <summary>
            /// Construct the mock event.
            /// </summary>
            /// <param name="sequenceNumber"></param>
            /// <param name="enqueuedTimeUtc"></param>
            /// <param name="offset"></param>
            /// <param name="partitionKey"></param>
            public EventDataMock(long sequenceNumber, DateTime enqueuedTimeUtc, string offset, string partitionKey)
            {
                this.SystemProperties = new SystemPropertiesCollectionMock(sequenceNumber, enqueuedTimeUtc, offset, partitionKey);
                this.Properties = new Dictionary<string, object>();
            }

            /// <summary>
            /// Body of the mock event.
            /// </summary>
            public ArraySegment<byte> Body { get; set; }

            /// <summary>
            /// User properties of the mock event.
            /// </summary>
            public IDictionary<string, object> Properties { get; set; }

            /// <summary>
            /// System properties of the mock event.
            /// </summary>
            public EventHubWrappers.ISystemPropertiesCollection SystemProperties { get; private set; }

            /// <summary>
            /// Disposing the mock does nothing.
            /// </summary>
            public void Dispose()
            {
            }
        }

        /// <summary>
        /// Mock of an Event Hub partition receiver.
        /// </summary>
        public class PartitionReceiverMock : EventHubWrappers.IPartitionReceiver
        {
            private readonly string partitionId;
            private long sequenceNumber;
            private EventHubWrappers.IPartitionReceiveHandler2 outerHandler;
            private bool invokeWhenNoEvents;
            private readonly CancellationToken token;

            /// <summary>
            /// Construct the partition receiver mock.
            /// </summary>
            /// <param name="partitionId"></param>
            /// <param name="sequenceNumber"></param>
            /// <param name="token"></param>
            public PartitionReceiverMock(string partitionId, long sequenceNumber, CancellationToken token)
            {
                this.partitionId = partitionId;
                this.sequenceNumber = sequenceNumber;
                this.token = token;
            }

            /// <summary>
            /// Receive mock events.
            /// </summary>
            /// <param name="maxEventCount"></param>
            /// <param name="waitTime"></param>
            /// <returns></returns>
            public Task<IEnumerable<EventHubWrappers.IEventData>> ReceiveAsync(int maxEventCount, TimeSpan waitTime)
            {
                List<EventDataMock> events = new List<EventDataMock>();
                for (int i = 0; i < maxEventCount; i++)
                {
                    this.sequenceNumber++;
                    EventDataMock e = new EventDataMock(this.sequenceNumber, DateTime.UtcNow, (this.sequenceNumber * 100).ToString(), this.partitionId);
                    e.Properties.Add("userkey", "uservalue");
                    byte[] body = new byte[] { 0x4D, 0x4F, 0x43, 0x4B, 0x42, 0x4F, 0x44, 0x59 }; // M O C K B O D Y
                    e.Body = new ArraySegment<byte>(body);
                    events.Add(e);
                }
                Thread.Sleep(5000);
                EventProcessorEventSource.Current.Message("MOCK ReceiveAsync returning {0} events for partition {1} ending at {2}", maxEventCount, this.partitionId, this.sequenceNumber);
                return Task.FromResult<IEnumerable<EventHubWrappers.IEventData>>(events);
            }

            /// <summary>
            /// Set a mock receive handler.
            /// </summary>
            /// <param name="receiveHandler"></param>
            /// <param name="invokeWhenNoEvents"></param>
            public void SetReceiveHandler(EventHubWrappers.IPartitionReceiveHandler2 receiveHandler, bool invokeWhenNoEvents = false)
            {
                EventProcessorEventSource.Current.Message("MOCK IPartitionReceiver.SetReceiveHandler");
                this.outerHandler = receiveHandler;
                this.invokeWhenNoEvents = invokeWhenNoEvents; // TODO mock does not emulate receive timeouts
                if (this.outerHandler != null)
                {
                    Task.Run(() => GenerateMessages());
                }
            }

            /// <summary>
            /// Close the mock receiver.
            /// </summary>
            /// <returns></returns>
            public Task CloseAsync()
            {
                EventProcessorEventSource.Current.Message("MOCK IPartitionReceiver.CloseAsync");
                return Task.CompletedTask;
            }

            private async void GenerateMessages()
            {
                while ((!this.token.IsCancellationRequested) && (this.outerHandler != null))
                {
                    EventProcessorEventSource.Current.Message("MOCK Generating messages and sending to handler");
                    IEnumerable<EventHubWrappers.IEventData> events = ReceiveAsync(10, TimeSpan.FromSeconds(10.0)).Result; // TODO get count from somewhere real
                    EventHubWrappers.IPartitionReceiveHandler2 capturedHandler = this.outerHandler;
                    if (capturedHandler != null)
                    {
                        await capturedHandler.ProcessEventsAsync(events);
                    }
                }
                EventProcessorEventSource.Current.Message("MOCK Message generation ending");
            }
        }

        /// <summary>
        /// Mock of EventHubClient class.
        /// </summary>
        public class EventHubClientMock : EventHubWrappers.IEventHubClient
        {
            private readonly int partitionCount;
            private readonly EventHubsConnectionStringBuilder csb;
            private CancellationToken token = new CancellationToken();

            /// <summary>
            /// Construct the mock.
            /// </summary>
            /// <param name="partitionCount"></param>
            /// <param name="csb"></param>
            public EventHubClientMock(int partitionCount, EventHubsConnectionStringBuilder csb)
            {
                this.partitionCount = partitionCount;
                this.csb = csb;
            }

            internal void SetCancellationToken(CancellationToken t)
            {
                this.token = t;
            }

            /// <summary>
            /// Get runtime info of the fake event hub.
            /// </summary>
            /// <returns></returns>
            public Task<EventHubRuntimeInformation> GetRuntimeInformationAsync()
            {
                EventHubRuntimeInformation ehri = new EventHubRuntimeInformation();
                ehri.PartitionCount = this.partitionCount;
                ehri.PartitionIds = new string[this.partitionCount];
                for (int i = 0; i < this.partitionCount; i++)
                {
                    ehri.PartitionIds[i] = i.ToString();
                }
                ehri.Path = csb.EntityPath;
                EventProcessorEventSource.Current.Message("MOCK GetRuntimeInformationAsync for {0}", ehri.Path);
                return Task.FromResult<EventHubRuntimeInformation>(ehri);
            }

            /// <summary>
            /// Create a mock receiver on the fake event hub.
            /// </summary>
            /// <param name="consumerGroupName"></param>
            /// <param name="partitionId"></param>
            /// <param name="eventPosition"></param>
            /// <param name="offset"></param>
            /// <param name="epoch"></param>
            /// <param name="receiverOptions"></param>
            /// <returns></returns>
            public EventHubWrappers.IPartitionReceiver CreateEpochReceiver(string consumerGroupName, string partitionId, EventPosition eventPosition, string offset, long epoch, ReceiverOptions receiverOptions)
            {
                EventProcessorEventSource.Current.Message("MOCK CreateEpochReceiver(CG {0}, part {1}, offset {2} epoch {3})", consumerGroupName, partitionId, offset, epoch);
                // TODO Mock does not implement epoch semantics
                long startSeq = (offset != null) ? (long.Parse(offset) / 100L) : 0L;
                return new PartitionReceiverMock(partitionId, startSeq, this.token);
            }

            /// <summary>
            /// Close the mock EventHubClient.
            /// </summary>
            /// <returns></returns>
            public Task CloseAsync()
            {
                EventProcessorEventSource.Current.Message("MOCK IEventHubClient.CloseAsync");
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// An EventHubClient factory which dispenses mocks.
        /// </summary>
        public class EventHubClientFactoryMock : EventHubWrappers.IEventHubClientFactory
        {
            private readonly int partitionCount;

            /// <summary>
            /// Construct the mock factory.
            /// </summary>
            /// <param name="partitionCount"></param>
            public EventHubClientFactoryMock(int partitionCount)
            {
                this.partitionCount = partitionCount;
            }

            /// <summary>
            /// Dispense a mock instance operating on a fake event hub with name taken from the connection string.
            /// </summary>
            /// <param name="connectionString"></param>
            /// <returns></returns>
            public EventHubWrappers.IEventHubClient CreateFromConnectionString(string connectionString)
            {
                EventProcessorEventSource.Current.Message("MOCK Creating IEventHubClient {0} with {1} partitions", connectionString, this.partitionCount);
                return new EventHubClientMock(this.partitionCount, new EventHubsConnectionStringBuilder(connectionString));
            }
        }
    }
}
