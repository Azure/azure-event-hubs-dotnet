namespace Microsoft.Azure.EventHubs.UnitTests
{
    using System;
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
            var csb = new EventHubsConnectionStringBuilder(this.EventHubConnectionString);
            csb.EntityPath = Guid.NewGuid().ToString();
            var ehClient = EventHubClient.CreateFromConnectionString(csb.ToString());

            // GetRuntimeInformationAsync on a nonexistent entity.
            await Assert.ThrowsAsync<MessagingEntityNotFoundException>(async () =>
            {
                Log("Getting partition information from a nonexistent entity.");
                var eventHubInfo = await ehClient.GetRuntimeInformationAsync();
                throw new InvalidOperationException("GetRuntimeInformation call should have failed");
            });

            // Try sending.
            PartitionSender sender = null;
            await Assert.ThrowsAsync<MessagingEntityNotFoundException>(async () =>
            {
                Log("Sending an event to nonexistent entity.");
                sender = ehClient.CreatePartitionSender("0");
                await sender.SendAsync(new EventData(Encoding.UTF8.GetBytes("this send should fail.")));
                throw new InvalidOperationException("Send should have failed");
            });
            await sender.CloseAsync();

            // Try receiving.
            PartitionReceiver receiver = null;
            await Assert.ThrowsAsync<MessagingEntityNotFoundException>(async () =>
            {
                Log("Receiving from nonexistent entity.");
                receiver = ehClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, "0", PartitionReceiver.StartOfStream);
                await receiver.ReceiveAsync(1);
                throw new InvalidOperationException("Receive should have failed");
            });
            await receiver.CloseAsync();

            // Try receiving on an nonexistent consumer group.
            ehClient = EventHubClient.CreateFromConnectionString(this.EventHubConnectionString);
            await Assert.ThrowsAsync<MessagingEntityNotFoundException>(async () =>
            {
                Log("Receiving from nonexistent consumer group.");
                receiver = ehClient.CreateReceiver(Guid.NewGuid().ToString(), "0", PartitionReceiver.StartOfStream);
                await receiver.ReceiveAsync(1);
                throw new InvalidOperationException("Receive should have failed");
            });
            await receiver.CloseAsync();
        }

        [Fact]
        Task CreateClientWithoutEntityPathShouldFail()
        {
            // Remove entity path from connection string.
            var csb = new EventHubsConnectionStringBuilder(this.EventHubConnectionString);
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
