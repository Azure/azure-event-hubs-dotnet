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
            PartitionReceiver partitionReceiver = this.EventHubClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, partitionId, EventPosition.FromEnqueuedTime(DateTime.UtcNow.AddMinutes(-10)));

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

        public virtual void Dispose()
        {
            this.EventHubClient.CloseAsync().GetAwaiter().GetResult();
        }
    }
}
