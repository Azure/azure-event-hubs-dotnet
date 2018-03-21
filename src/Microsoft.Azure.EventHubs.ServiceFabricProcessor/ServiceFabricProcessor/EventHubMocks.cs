// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System;
using System.Collections.Generic;
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
            private string partitionId;
            private long sequenceNumber;

            public PartitionReceiverMock(string partitionId, long sequenceNumber)
            {
                this.partitionId = partitionId;
                this.sequenceNumber = sequenceNumber;
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

            public Task CloseAsync()
            {
                EventProcessorEventSource.Current.Message("MOCK IPartitionReceiver.CloseAsync");
                return Task.CompletedTask;
            }
        }

        public class EventHubClientMock : EventHubWrappers.IEventHubClient
        {
            private int partitionCount;
            private EventHubsConnectionStringBuilder csb;

            public EventHubClientMock(int partitionCount, EventHubsConnectionStringBuilder csb)
            {
                this.partitionCount = partitionCount;
                this.csb = csb;
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
                return new PartitionReceiverMock(partitionId, eventPosition.SequenceNumber ?? 0L);
            }

            public Task CloseAsync()
            {
                EventProcessorEventSource.Current.Message("MOCK IEventHubClient.CloseAsync");
                return Task.CompletedTask;
            }
        }

        public class EventHubClientFactoryMock : EventHubWrappers.IEventHubClientFactory
        {
            private int partitionCount;

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
