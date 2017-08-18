// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Tests.Client
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using Xunit;

    public class MiscTests : ClientTestBase
    {
        [Fact]
        [DisplayTestMethodName]
        async Task PartitionKeyValidation()
        {
            int NumberOfMessagesToSend = 100;
            var partitionOffsets = new Dictionary<string, string>();

            // Discover the end of stream on each partition.
            TestUtility.Log("Discovering end of stream on each partition.");
            foreach (var partitionId in this.PartitionIds)
            {
                var lastEvent = await this.EventHubClient.GetPartitionRuntimeInformationAsync(partitionId);
                partitionOffsets.Add(partitionId, lastEvent.LastEnqueuedOffset);
                TestUtility.Log($"Partition {partitionId} has last message with offset {lastEvent.LastEnqueuedOffset}");
            }

            // Now send a set of messages with different partition keys.
            TestUtility.Log($"Sending {NumberOfMessagesToSend} messages.");
            Random rnd = new Random();
            for (int i = 0; i < NumberOfMessagesToSend; i++)
            {
                var partitionKey = rnd.Next(10);
                await this.EventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes("Hello EventHub!")), partitionKey.ToString());
            }

            // It is time to receive all messages that we just sent.
            // Prepare partition key to partition map while receiving.
            // Validation: All messages of a partition key should be received from a single partition.
            TestUtility.Log("Starting to receive all messages from each partition.");
            var partitionMap = new Dictionary<string, string>();
            int totalReceived = 0;
            foreach (var partitionId in this.PartitionIds)
            {
                PartitionReceiver receiver = null;
                try
                {
                    receiver = this.EventHubClient.CreateReceiver(
                        PartitionReceiver.DefaultConsumerGroupName,
                        partitionId,
                        partitionOffsets[partitionId]);
                    var messagesFromPartition = await ReceiveAllMessages(receiver);
                    TestUtility.Log($"Received {messagesFromPartition.Count} messages from partition {partitionId}.");
                    foreach (var ed in messagesFromPartition)
                    {
                        var pk = ed.SystemProperties.PartitionKey;
                        if (partitionMap.ContainsKey(pk) && partitionMap[pk] != partitionId)
                        {
                            throw new Exception($"Received a message from partition {partitionId} with partition key {pk}, whereas the same key was observed on partition {partitionMap[pk]} before.");
                        }

                        partitionMap[pk] = partitionId;
                    }

                    totalReceived += messagesFromPartition.Count;
                }
                finally
                {
                    await receiver.CloseAsync();
                }
            }

            Assert.True(totalReceived == NumberOfMessagesToSend,
                $"Didn't receive the same number of messages that we sent. Sent: {NumberOfMessagesToSend}, Received: {totalReceived}");
        }

        [Fact]
        [DisplayTestMethodName]
        async Task UseSharedAccessSignature()
        {
            // Generate shared access token.
            var csb = new EventHubsConnectionStringBuilder(TestUtility.EventHubsConnectionString);
            var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(csb.SasKeyName, csb.SasKey);
            var token = await tokenProvider.GetTokenAsync(csb.Endpoint.ToString(), "Send,Receive", TimeSpan.FromSeconds(120));
            var sas = token.TokenValue.ToString();

            // Update connection string builder to use shared access signature instead.
            csb.SasKey = "";
            csb.SasKeyName = "";
            csb.SharedAccessSignature = sas;

            // Create new client with updated connection string.
            var ehClient = EventHubClient.CreateFromConnectionString(csb.ToString());

            // Send one event
            TestUtility.Log($"Sending one message.");
            var ehSender = ehClient.CreatePartitionSender("0");
            var eventData = new EventData(Encoding.UTF8.GetBytes("Hello EventHub by partitionKey!"));
            await ehSender.SendAsync(eventData);

            // Receive event.
            TestUtility.Log($"Receiving one message.");
            var ehReceiver = ehClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, "0", PartitionReceiver.StartOfStream);
            var msg = await ehReceiver.ReceiveAsync(1);
            Assert.True(msg != null, "Failed to receive message.");

            // Get EH runtime information.
            TestUtility.Log($"Getting Event Hub runtime information.");
            var ehInfo = await ehClient.GetRuntimeInformationAsync();
            Assert.True(ehInfo != null, "Failed to get runtime information.");

            // Get EH partition runtime information.
            TestUtility.Log($"Getting Event Hub partition '0' runtime information.");
            var partitionInfo = await ehClient.GetPartitionRuntimeInformationAsync("0");
            Assert.True(ehInfo != null, "Failed to get runtime partition information.");
        }

        [Fact]
        [DisplayTestMethodName]
        async Task SendAndReceiveLargeMessage()
        {
            var bodySize = 250 * 1024;
            var targetPartition = "0";

            var edToSend = new EventData(new byte[bodySize]);

            TestUtility.Log($"Sending one message with body size {bodySize} bytes.");
            var edReceived = await SendAndReceiveEvent(targetPartition, edToSend);

            // Validate array segment count.
            Assert.True(edReceived.Body.Count == bodySize, $"Sent {bodySize} bytes and received {edReceived.Body.Count}");
        }
    }
}
