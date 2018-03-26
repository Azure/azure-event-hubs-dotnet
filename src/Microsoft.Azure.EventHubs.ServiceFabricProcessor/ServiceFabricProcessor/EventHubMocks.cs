// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    public class EventHubMocks
    {
        public class SystemPropertiesCollectionMock : EventHubWrappers.ISystemPropertiesCollection
        {
            public SystemPropertiesCollectionMock(long sequenceNumber, DateTime enqueuedTimeUtc, string offset, string partitionKey)
            {
                this.SequenceNumber = sequenceNumber;
                this.EnqueuedTimeUtc = enqueuedTimeUtc;
                this.Offset = offset;
                this.PartitionKey = partitionKey;
            }

            public long SequenceNumber { get; private set; }

            public DateTime EnqueuedTimeUtc { get; private set; }

            public string Offset { get; private set; }

            public string PartitionKey { get; private set; }
        }

        public class EventDataMock : EventHubWrappers.IEventData
        {
            public EventDataMock(long sequenceNumber, DateTime enqueuedTimeUtc, string offset, string partitionKey)
            {
                this.SystemProperties = new SystemPropertiesCollectionMock(sequenceNumber, enqueuedTimeUtc, offset, partitionKey);
            }

            public ArraySegment<byte> Body { get; set; }

            public IDictionary<string, object> Properties { get; set; }

            public EventHubWrappers.ISystemPropertiesCollection SystemProperties { get; private set; }

            public void Dispose()
            {
            }
        }

        public class PartitionReceiverMock : EventHubWrappers.IPartitionReceiver
        {
            private readonly string partitionId;
            private long sequenceNumber;
            private EventHubWrappers.IPartitionReceiveHandler2 outerHandler;
            private bool invokeWhenNoEvents;
            private readonly CancellationToken token;

            public PartitionReceiverMock(string partitionId, long sequenceNumber, CancellationToken token)
            {
                this.partitionId = partitionId;
                this.sequenceNumber = sequenceNumber;
                this.token = token;
            }

            public Task<IEnumerable<EventHubWrappers.IEventData>> ReceiveAsync(int maxEventCount, TimeSpan waitTime)
            {
                List<EventDataMock> events = new List<EventDataMock>();
                for (int i = 0; i < maxEventCount; i++)
                {
                    this.sequenceNumber++;
                    events.Add(new EventDataMock(this.sequenceNumber, DateTime.UtcNow, this.sequenceNumber.ToString(), this.partitionId));
                }
                System.Threading.Thread.Sleep(5000);
                EventProcessorEventSource.Current.Message("MOCK ReceiveAsync returning {0} events for partition {1} ending at {2}", maxEventCount, this.partitionId, this.sequenceNumber);
                return Task.FromResult<IEnumerable<EventHubWrappers.IEventData>>(events);
            }

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

            public Task CloseAsync()
            {
                EventProcessorEventSource.Current.Message("MOCK IPartitionReceiver.CloseAsync");
                return Task.CompletedTask;
            }

            private void GenerateMessages()
            {
                while ((!this.token.IsCancellationRequested) && (this.outerHandler != null))
                {
                    EventProcessorEventSource.Current.Message("MOCK Generating messages and sending to handler");
                    IEnumerable<EventHubWrappers.IEventData> events = ReceiveAsync(10, TimeSpan.FromSeconds(10.0)).Result; // TODO get count from somewhere real
                    EventHubWrappers.IPartitionReceiveHandler2 capturedHandler = this.outerHandler;
                    if (capturedHandler != null)
                    {
                        capturedHandler.ProcessEventsAsync(events);
                    }
                }
                EventProcessorEventSource.Current.Message("MOCK Message generation ending");
            }
        }

        public class EventHubClientMock : EventHubWrappers.IEventHubClient
        {
            private readonly int partitionCount;
            private readonly EventHubsConnectionStringBuilder csb;
            private CancellationToken token = new CancellationToken();

            public EventHubClientMock(int partitionCount, EventHubsConnectionStringBuilder csb)
            {
                this.partitionCount = partitionCount;
                this.csb = csb;
            }

            internal void SetCancellationToken(CancellationToken t)
            {
                this.token = t;
            }

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

            public EventHubWrappers.IPartitionReceiver CreateEpochReceiver(string consumerGroupName, string partitionId, EventPosition eventPosition, long epoch, ReceiverOptions receiverOptions)
            {
                EventProcessorEventSource.Current.Message("MOCK CreateEpochReceiver(CG {0}, part {1}, epoch {3})", consumerGroupName, partitionId, eventPosition.SequenceNumber, epoch);
                // TODO Mock does not implement epoch semantics
                return new PartitionReceiverMock(partitionId, eventPosition.SequenceNumber ?? 0L, this.token);
            }

            public Task CloseAsync()
            {
                EventProcessorEventSource.Current.Message("MOCK IEventHubClient.CloseAsync");
                return Task.CompletedTask;
            }
        }

        public class EventHubClientFactoryMock : EventHubWrappers.IEventHubClientFactory
        {
            private readonly int partitionCount;

            public EventHubClientFactoryMock(int partitionCount)
            {
                this.partitionCount = partitionCount;
            }

            public EventHubWrappers.IEventHubClient CreateFromConnectionString(string connectionString)
            {
                EventProcessorEventSource.Current.Message("MOCK Creating IEventHubClient {0} with {1} partitions", connectionString, this.partitionCount);
                return new EventHubClientMock(this.partitionCount, new EventHubsConnectionStringBuilder(connectionString));
            }
        }
    }
}
