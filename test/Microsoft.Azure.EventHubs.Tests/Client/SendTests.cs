// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Tests.Client
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Xunit;

    public class SendTests : ClientTestBase
    {
        [Fact]
        [DisplayTestMethodName]
        async Task SendAndReceiveZeroLengthBody()
        {
            var targetPartition = "0";
            var zeroBodyEventData = new EventData(new byte[0]);
            var edReceived = await SendAndReceiveEvent(targetPartition, zeroBodyEventData);

            // Validate body.
            Assert.True(edReceived.Body.Count == 0, $"Received event's body isn't zero byte long.");
        }

        [Fact]
        [DisplayTestMethodName]
        Task SendSingleEvent()
        {
            TestUtility.Log("Sending single Event via EventHubClient.SendAsync(EventData, string)");
            var eventData = new EventData(Encoding.UTF8.GetBytes("Hello EventHub by partitionKey!"));
            return this.EventHubClient.SendAsync(eventData, "SomePartitionKeyHere");
        }

        [Fact]
        [DisplayTestMethodName]
        Task SendBatch()
        {
            TestUtility.Log("Sending multiple Events via EventHubClient.SendAsync(IEnumerable<EventData>)");
            var eventData1 = new EventData(Encoding.UTF8.GetBytes("Hello EventHub!"));
            var eventData2 = new EventData(Encoding.UTF8.GetBytes("This is another message in the batch!"));
            eventData2.Properties["ContosoEventType"] = "some value here";
            return this.EventHubClient.SendAsync(new[] { eventData1, eventData2 });
        }

        [Fact]
        [DisplayTestMethodName]
        async Task PartitionSenderSend()
        {
            TestUtility.Log("Sending single Event via PartitionSender.SendAsync(EventData)");
            PartitionSender partitionSender1 = this.EventHubClient.CreatePartitionSender("1");
            try
            {
                var eventData = new EventData(Encoding.UTF8.GetBytes("Hello again EventHub Partition 1!"));
                await partitionSender1.SendAsync(eventData);
            }
            finally
            {
                await partitionSender1.CloseAsync();
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task PartitionSenderSendBatch()
        {
            TestUtility.Log("Sending single Event via PartitionSender.SendAsync(IEnumerable<EventData>)");
            PartitionSender partitionSender1 = this.EventHubClient.CreatePartitionSender("1");
            try
            {
                var eventData1 = new EventData(Encoding.UTF8.GetBytes("Hello EventHub!"));
                var eventData2 = new EventData(Encoding.UTF8.GetBytes("This is another message in the batch!"));
                eventData2.Properties["ContosoEventType"] = "some value here";
                await partitionSender1.SendAsync(new[] { eventData1, eventData2 });
            }
            finally
            {
                await partitionSender1.CloseAsync();
            }
        }

        /// <summary>
        /// Utilizes EventDataBatch to send messages as the messages are batched up to max batch size.
        /// </summary>
        [Fact]
        [DisplayTestMethodName]
        async Task BatchSender()
        {
            await SendWithEventDataBatch();
        }

        /// <summary>
        /// Utilizes EventDataBatch to send messages as the messages are batched up to max batch size.
        /// This unit test sends with partition key.
        /// </summary>
        [Fact]
        [DisplayTestMethodName]
        async Task BatchSenderWithPartitionKey()
        {
            await SendWithEventDataBatch(Guid.NewGuid().ToString());
        }

        [Fact]
        [DisplayTestMethodName]
        async Task SendAndReceiveArraySegmentEventData()
        {
            var targetPartition = "0";
            byte[] byteArr = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

            var edToSend = new EventData(new ArraySegment<byte>(byteArr));
            var edReceived = await SendAndReceiveEvent(targetPartition, edToSend);

            // Validate array segment count.
            Assert.True(edReceived.Body.Count == byteArr.Count(), $"Sent {byteArr.Count()} bytes and received {edReceived.Body.Count}");
        }

        [Fact]
        [DisplayTestMethodName]
        async Task MultipleClientsSend()
        {
            var maxNumberOfClients = 100;
            var syncEvent = new ManualResetEventSlim(false);

            TestUtility.Log($"Starting {maxNumberOfClients} SendAsync tasks in parallel.");

            var tasks = new List<Task>();
            for (var i = 0; i < maxNumberOfClients; i++)
            {
                var task = Task.Run(async () =>
                {
                    syncEvent.Wait();
                    var ehClient = EventHubClient.CreateFromConnectionString(TestUtility.EventHubsConnectionString);
                    await ehClient.SendAsync(new EventData(Encoding.UTF8.GetBytes("Hello EventHub!")));
                });

                tasks.Add(task);
            }

            var waitForAccountToInitialize = Task.Delay(10000);
            await waitForAccountToInitialize;
            syncEvent.Set();
            await Task.WhenAll(tasks);

            TestUtility.Log("All Send tasks have completed.");
        }

        [Fact]
        [DisplayTestMethodName]
        async Task CloseSenderClient()
        {
            var pSender = this.EventHubClient.CreatePartitionSender("0");
            var pReceiver = this.EventHubClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, "0", PartitionReceiver.StartOfStream);

            try
            {
                TestUtility.Log("Sending single event to partition 0");
                var eventData = new EventData(Encoding.UTF8.GetBytes("Hello EventHub!"));
                await pSender.SendAsync(eventData);

                TestUtility.Log("Closing partition sender");
                await pSender.CloseAsync();

                await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
                {
                    TestUtility.Log("Sending another event to partition 0 on the closed sender, this should fail");
                    eventData = new EventData(Encoding.UTF8.GetBytes("Hello EventHub!"));
                    await pSender.SendAsync(eventData);
                    throw new InvalidOperationException("Send should have failed");
                });
            }
            finally
            {
                await pReceiver.CloseAsync();
            }
        }
    }
}
