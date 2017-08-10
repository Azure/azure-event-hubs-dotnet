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

    public class WebSocketTests : ClientTestBase
    {
        string webSocketConnString;

        public WebSocketTests()
        {
            // Create connection string builder with web-sockets enabled.
            var csb = new EventHubsConnectionStringBuilder(TestUtility.EventHubsConnectionString);
            csb.TransportType = TransportType.AmqpWebSockets;
            csb.OperationTimeout = TimeSpan.FromMinutes(5);

            // Confirm connection string has transport-type set as desired.
            this.webSocketConnString = csb.ToString();

            // Remove secrets.
            csb.SasKey = "XXX";
            var webSocketConnStringTest = csb.ToString();

            Assert.True(webSocketConnString.Contains("TransportType=AmqpWebSockets"),
                $"Web-sockets enabled connection string doesn't contain desired setting: {webSocketConnStringTest}");
        }

        [Fact]
        [DisplayTestMethodName]
        async Task GetEventHubRuntimeInformation()
        {
            var ehClient = EventHubClient.CreateFromConnectionString(webSocketConnString);

            TestUtility.Log("Getting  EventHubRuntimeInformation");
            var eventHubRuntimeInformation = await ehClient.GetRuntimeInformationAsync();

            Assert.True(eventHubRuntimeInformation != null, "eventHubRuntimeInformation was null!");
            Assert.True(eventHubRuntimeInformation.PartitionIds != null, "eventHubRuntimeInformation.PartitionIds was null!");
            Assert.True(eventHubRuntimeInformation.PartitionIds.Length != 0, "eventHubRuntimeInformation.PartitionIds.Length was 0!");

            TestUtility.Log("Found partitions:");
            foreach (string partitionId in eventHubRuntimeInformation.PartitionIds)
            {
                TestUtility.Log(partitionId);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task SendAndReceive()
        {
            string targetPartitionId = "0";

            // Create new client with updated connection string.
            TestUtility.Log("Creating Event Hub client");
            var ehClient = EventHubClient.CreateFromConnectionString(webSocketConnString);

            // Send single message
            TestUtility.Log("Sending single event");
            var sender = ehClient.CreatePartitionSender(targetPartitionId);
            var eventData = new EventData(Encoding.UTF8.GetBytes("This event will be transported via web-sockets"));
            await sender.SendAsync(eventData);

            // Receive single message.
            TestUtility.Log("Receiving single event");
            var receiver = ehClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, targetPartitionId, PartitionReceiver.StartOfStream);
            var msg = await receiver.ReceiveAsync(1);

            Assert.True(msg != null, $"Failed to receive single event from partition {targetPartitionId}");
        }
    }
}
