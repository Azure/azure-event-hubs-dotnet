// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Tests.Client
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Xunit;

    public class ClientTestBase : IDisposable
    {
        protected string[] PartitionIds;
        protected EventHubClient EventHubClient;

        public ClientTestBase()
        {
            // Create default EH client.
            this.EventHubClient = EventHubClient.CreateFromConnectionString(TestUtility.EventHubsConnectionString);
            
            // Discover partition ids.
            var eventHubInfo = this.EventHubClient.GetRuntimeInformationAsync().Result;
            this.PartitionIds = eventHubInfo.PartitionIds;
            TestUtility.Log($"EventHub has {PartitionIds.Length} partitions");
        }
        
        // Send and receive given event on given partition.
        protected async Task<EventData> SendAndReceiveEvent(string partitionId, EventData sendEvent)
        {
            EventData receivedEvent = null;
            PartitionSender partitionSender = this.EventHubClient.CreatePartitionSender(partitionId);
            PartitionReceiver partitionReceiver = this.EventHubClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, partitionId, DateTime.UtcNow.AddMinutes(-10));

            try
            {
                string uniqueEventId = Guid.NewGuid().ToString();
                TestUtility.Log($"Sending event to Partition {partitionId} with custom property EventId {uniqueEventId}");
                sendEvent.Properties["EventId"] = uniqueEventId;
                await partitionSender.SendAsync(sendEvent);

                bool expectedEventReceived = false;
                do
                {
                    IEnumerable<EventData> eventDatas = await partitionReceiver.ReceiveAsync(10);
                    if (eventDatas == null)
                    {
                        break;
                    }

                    TestUtility.Log($"Received a batch of {eventDatas.Count()} events:");
                    foreach (var eventData in eventDatas)
                    {
                        object objectValue;
                        if (eventData.Properties != null && eventData.Properties.TryGetValue("EventId", out objectValue))
                        {
                            TestUtility.Log($"Received message with EventId {objectValue}");
                            string receivedId = objectValue.ToString();
                            if (receivedId == uniqueEventId)
                            {
                                TestUtility.Log("Success");
                                receivedEvent = eventData;
                                expectedEventReceived = true;
                                break;
                            }
                        }
                    }
                }
                while (!expectedEventReceived);

                Assert.True(expectedEventReceived, $"Did not receive expected event with EventId {uniqueEventId}");
            }
            finally
            {
                await Task.WhenAll(
                    partitionReceiver.CloseAsync(),
                    partitionSender.CloseAsync());
            }

            return receivedEvent;
        }

        // Receives all messages on the given receiver.
        protected async Task<List<EventData>> ReceiveAllMessages(PartitionReceiver receiver)
        {
            List<EventData> messages = new List<EventData>();

            while (true)
            {
                var receivedEvents = await receiver.ReceiveAsync(100);
                if (receivedEvents == null)
                {
                    // There is no more events to receive.
                    break;
                }

                messages.AddRange(receivedEvents);
            }

            return messages;
        }

        protected async Task SendWithEventDataBatch(string partitionKey = null)
        {
            const int MinimumNumberOfMessagesToSend = 1000;

            var receivers = new List<PartitionReceiver>();

            // Create partition receivers starting from the end of the stream.
            TestUtility.Log("Discovering end of stream on each partition.");
            foreach (var partitionId in this.PartitionIds)
            {
                var lastEvent = await this.EventHubClient.GetPartitionRuntimeInformationAsync(partitionId);
                receivers.Add(this.EventHubClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, partitionId, lastEvent.LastEnqueuedOffset));
            }

            try
            {
                // Start receicing messages now.
                var receiverTasks = new List<Task<List<EventData>>>();
                foreach (var receiver in receivers)
                {
                    receiverTasks.Add(ReceiveAllMessages(receiver));
                }

                // Create initial batcher.
                EventDataBatch batcher = this.EventHubClient.CreateBatch();

                // We will send a thousand messages where each message is 1K.
                var totalSent = 0;
                var rnd = new Random();
                TestUtility.Log($"Starting to send.");
                do
                {
                    // Send random body size.
                    var ed = new EventData(new byte[rnd.Next(0, 1024)]);
                    if (!batcher.TryAdd(ed))
                    {
                        // Time to send the batch.
                        if (partitionKey != null)
                        {
                            await this.EventHubClient.SendAsync(batcher.ToEnumerable(), partitionKey);
                        }
                        else
                        {
                            await this.EventHubClient.SendAsync(batcher.ToEnumerable());
                        }

                        totalSent += batcher.Count;
                        TestUtility.Log($"Sent {batcher.Count} messages in the batch.");

                        // Create new batcher.
                        batcher = this.EventHubClient.CreateBatch();
                    }
                } while (totalSent < MinimumNumberOfMessagesToSend);

                // Send the rest of the batch if any.
                if (batcher.Count > 0)
                {
                    await this.EventHubClient.SendAsync(batcher.ToEnumerable());
                    totalSent += batcher.Count;
                    TestUtility.Log($"Sent {batcher.Count} messages in the batch.");
                }

                TestUtility.Log($"{totalSent} messages sent in total.");

                var pReceived = await Task.WhenAll(receiverTasks);
                var totalReceived = pReceived.Sum(p => p.Count);
                TestUtility.Log($"{totalReceived} messages received in total.");

                // All messages received?
                Assert.True(totalReceived == totalSent, $"Failed receive {totalSent}, but received {totalReceived} messages.");

                // If partition key is set then we expect all messages from the same partition.
                if (partitionKey != null)
                {
                    Assert.True(pReceived.Count(p => p.Count > 0) == 1, "Received messsages from multiple partitions.");
                }
            }
            finally
            {
                await Task.WhenAll(receivers.Select(r => r.CloseAsync()));
            }
        }

        public void Dispose()
        {
            this.EventHubClient.CloseAsync().GetAwaiter().GetResult();
        }
    }
}
