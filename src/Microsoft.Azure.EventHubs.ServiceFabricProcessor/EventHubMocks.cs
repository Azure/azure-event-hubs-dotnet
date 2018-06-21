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
        /// Mock of an Event Hub partition receiver.
        /// </summary>
        public class PartitionReceiverMock : EventHubWrappers.IPartitionReceiver
        {
            private readonly string partitionId;
            private long sequenceNumber;
            private IPartitionReceiveHandler outerHandler;
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
            public Task<IEnumerable<EventData>> ReceiveAsync(int maxEventCount, TimeSpan waitTime)
            {
                List<EventData> events = new List<EventData>();
                for (int i = 0; i < maxEventCount; i++)
                {
                    this.sequenceNumber++;
                    byte[] body = new byte[] { 0x4D, 0x4F, 0x43, 0x4B, 0x42, 0x4F, 0x44, 0x59 }; // M O C K B O D Y
                    EventData e = new EventData(body);
                    e.SystemProperties.Add(ClientConstants.SequenceNumberName, this.sequenceNumber);
                    e.SystemProperties.Add(ClientConstants.EnqueuedTimeUtcName, DateTime.UtcNow);
                    e.SystemProperties.Add(ClientConstants.OffsetName, (this.sequenceNumber * 100).ToString());
                    e.SystemProperties.Add(ClientConstants.PartitionKeyName, "");
                    e.Properties.Add("userkey", "uservalue");
                    events.Add(e);
                }
                Thread.Sleep(5000);
                EventProcessorEventSource.Current.Message("MOCK ReceiveAsync returning {0} events for partition {1} ending at {2}", maxEventCount, this.partitionId, this.sequenceNumber);
                return Task.FromResult<IEnumerable<EventData>>(events);
            }

            /// <summary>
            /// Set a mock receive handler.
            /// </summary>
            /// <param name="receiveHandler"></param>
            /// <param name="invokeWhenNoEvents"></param>
            public void SetReceiveHandler(IPartitionReceiveHandler receiveHandler, bool invokeWhenNoEvents = false)
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
                    IEnumerable<EventData> events = ReceiveAsync(10, TimeSpan.FromSeconds(10.0)).Result; // TODO get count from somewhere real
                    IPartitionReceiveHandler capturedHandler = this.outerHandler;
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
