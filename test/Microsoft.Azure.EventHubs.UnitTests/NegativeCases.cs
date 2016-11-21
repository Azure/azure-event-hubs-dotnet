// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;

    public class NegativeCases : EventHubClientTests
    {
        public NegativeCases(ITestOutputHelper output)
            : base(output)
        { }

        [Fact]
        async Task NonexistentEntity()
        {
            // Rebuild connection string with a nonexistent entity.
            var csb = new EventHubsConnectionStringBuilder(this.EventHubsConnectionString);
            csb.EntityPath = Guid.NewGuid().ToString();
            var ehClient = EventHubClient.CreateFromConnectionString(csb.ToString());

            // GetRuntimeInformationAsync on a nonexistent entity.
            await Assert.ThrowsAsync<MessagingEntityNotFoundException>(async () =>
            {
                Log("Getting entity information from a nonexistent entity.");
                await ehClient.GetRuntimeInformationAsync();
                throw new InvalidOperationException("GetRuntimeInformation call should have failed");
            });

            // GetPartitionRuntimeInformationAsync on a nonexistent entity.
            await Assert.ThrowsAsync<MessagingEntityNotFoundException>(async () =>
            {
                Log("Getting partition information from a nonexistent entity.");
                await ehClient.GetPartitionRuntimeInformationAsync("0");
                throw new InvalidOperationException("GetPartitionRuntimeInformation call should have failed");
            });

            // Try sending.
            PartitionSender sender = null;
            await Assert.ThrowsAsync<MessagingEntityNotFoundException>(async () =>
            {
                Log("Sending an event to nonexistent entity.");
                sender = ehClient.CreatePartitionSender("0");
                await sender.SendAsync(new EventData(Encoding.UTF8.GetBytes("this send should fail.")));
                throw new InvalidOperationException("Send call should have failed");
            });
            await sender.CloseAsync();

            // Try receiving.
            PartitionReceiver receiver = null;
            await Assert.ThrowsAsync<MessagingEntityNotFoundException>(async () =>
            {
                Log("Receiving from nonexistent entity.");
                receiver = ehClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, "0", PartitionReceiver.StartOfStream);
                await receiver.ReceiveAsync(1);
                throw new InvalidOperationException("Receive call should have failed");
            });
            await receiver.CloseAsync();

            // Try receiving on an nonexistent consumer group.
            ehClient = EventHubClient.CreateFromConnectionString(this.EventHubsConnectionString);
            await Assert.ThrowsAsync<MessagingEntityNotFoundException>(async () =>
            {
                Log("Receiving from nonexistent consumer group.");
                receiver = ehClient.CreateReceiver(Guid.NewGuid().ToString(), "0", PartitionReceiver.StartOfStream);
                await receiver.ReceiveAsync(1);
                throw new InvalidOperationException("Receive call should have failed");
            });
            await receiver.CloseAsync();
        }

        [Fact]
        async Task ReceiveFromInvalidPartition()
        {
            PartitionReceiver receiver = null;

            // Some invalid partition values. These will fail on the service side.
            var invalidPartitions = new List<string>() { "XYZ", "-1", "1000", "-" };
            
            foreach (var invalidPartitionId in invalidPartitions)
            {
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                {
                    Log($"Receiving from invalid partition {invalidPartitionId}");
                    receiver = this.EventHubClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, invalidPartitionId, PartitionReceiver.StartOfStream);
                    await receiver.ReceiveAsync(1);
                    throw new InvalidOperationException("Receive call should have failed");
                });
                await receiver.CloseAsync();
            }

            // Some invalid partition values. These will fail on the client side.
            invalidPartitions = new List<string>() { "", " ", null };
            foreach (var invalidPartitionId in invalidPartitions)
            {
                await Assert.ThrowsAsync<ArgumentException>(async () =>
                {
                    Log($"Receiving from invalid partition {invalidPartitionId}");
                    receiver = this.EventHubClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, invalidPartitionId, PartitionReceiver.StartOfStream);
                    await receiver.ReceiveAsync(1);
                    throw new InvalidOperationException("Receive call should have failed");
                });
                await receiver.CloseAsync();
            }
        }

        [Fact]
        async Task SendToInvalidPartition()
        {
            PartitionSender sender = null;

            // Some invalid partition values.
            var invalidPartitions = new List<string>() { "XYZ", "-1", "1000", "-" };

            foreach (var invalidPartitionId in invalidPartitions)
            {
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                {
                    Log($"Sending to invalid partition {invalidPartitionId}");
                    sender = this.EventHubClient.CreatePartitionSender(invalidPartitionId);
                    await sender.SendAsync(new EventData(new byte[1]));
                    throw new InvalidOperationException("Send call should have failed");
                });
                await sender.CloseAsync();
            }

            // Some other invalid partition values. These will fail on the client side.
            invalidPartitions = new List<string>() { "", " ", null };
            foreach (var invalidPartitionId in invalidPartitions)
            {
                await Assert.ThrowsAsync<ArgumentException>(async () =>
                {
                    Log($"Sending to invalid partition {invalidPartitionId}");
                    sender = this.EventHubClient.CreatePartitionSender(invalidPartitionId);
                    await sender.SendAsync(new EventData(new byte[1]));
                    throw new InvalidOperationException("Send call should have failed");
                });
                await sender.CloseAsync();
            }
        }

        [Fact]
        async Task GetPartitionRuntimeInformationFromInvalidPartition()
        {
            // Some invalid partition values. These will fail on the service side.
            var invalidPartitions = new List<string>() { "XYZ", "-1", "1000", "-" };

            foreach (var invalidPartitionId in invalidPartitions)
            {
                await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                {
                    Log($"Getting partition information from invalid partition {invalidPartitionId}");
                    await this.EventHubClient.GetPartitionRuntimeInformationAsync(invalidPartitionId);
                    throw new InvalidOperationException("GetPartitionRuntimeInformation call should have failed");
                });
            }

            // Some other invalid partition values. These will fail on the client side.
            invalidPartitions = new List<string>() { "", " ", null };
            foreach (var invalidPartitionId in invalidPartitions)
            {
                await Assert.ThrowsAsync<ArgumentException>(async () =>
                {
                    Log($"Getting partition information from invalid partition {invalidPartitionId}");
                    await this.EventHubClient.GetPartitionRuntimeInformationAsync(invalidPartitionId);
                    throw new InvalidOperationException("GetPartitionRuntimeInformation call should have failed");
                });
            }
        }

        [Fact]
        Task CreateClientWithoutEntityPathShouldFail()
        {
            // Remove entity path from connection string.
            var csb = new EventHubsConnectionStringBuilder(this.EventHubsConnectionString);
            csb.EntityPath = null;

            return Assert.ThrowsAsync<ArgumentException>(() =>
            {
                EventHubClient.CreateFromConnectionString(csb.ToString());
                throw new Exception("Entity path wasn't provided in the connection string and this new call was supposed to fail");
            });
        }

        [Fact]
        async Task MessageSizeExceededException()
        {
            try
            {
                Log("Sending large event via EventHubClient.SendAsync(EventData)");
                var eventData = new EventData(new byte[300000]);
                await this.EventHubClient.SendAsync(eventData);
                throw new InvalidOperationException("Send should have failed with " +
                    typeof(MessageSizeExceededException).Name);
            }
            catch (MessageSizeExceededException)
            {
                Log("Caught MessageSizeExceededException as expected");
            }
        }
    }
}
