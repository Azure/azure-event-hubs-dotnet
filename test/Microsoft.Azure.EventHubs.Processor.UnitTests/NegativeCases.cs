// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Processor.UnitTests
{
    using System;
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;

    public class NegativeCases : EventProcessorHostTests
    {
        public NegativeCases(ITestOutputHelper output)
            : base(output)
        { }

        [Fact]
        async Task HostReregisterShouldFail()
        {
            var eventProcessorHost = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                this.EventHubConnectionString,
                this.StorageConnectionString,
                this.LeaseContainerName);

            // Calling register for the first time should succeed.
            Log("Registering EventProcessorHost for the first time.");
            await eventProcessorHost.RegisterEventProcessorAsync<TestEventProcessor>();

            try
            {
                // Calling register for the second time should fail.
                Log("Registering EventProcessorHost for the second time which should fail.");
                await eventProcessorHost.RegisterEventProcessorAsync<TestEventProcessor>();
                throw new InvalidOperationException("Second RegisterEventProcessorAsync call should have failed.");
            }
            catch (InvalidOperationException ex)
            {
                if (ex.Message.Contains("A PartitionManager cannot be started multiple times."))
                {
                    Log($"Caught {ex.GetType()} as expected");
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                await eventProcessorHost.UnregisterEventProcessorAsync();
            }
        }

        [Fact]
        async Task NonexsistentEntity()
        {
            // Rebuild connection string with a nonexistent entity.
            var csb = new EventHubsConnectionStringBuilder(this.EventHubConnectionString);
            csb.EntityPath = Guid.NewGuid().ToString();

            var eventProcessorHost = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                csb.ToString(),
                this.StorageConnectionString,
                this.LeaseContainerName);

            Log("Calling RegisterEventProcessorAsync for a nonexistent entity.");
            var ex = await Assert.ThrowsAsync<EventProcessorConfigurationException>(async () =>
            {
                await eventProcessorHost.RegisterEventProcessorAsync<TestEventProcessor>();
                throw new InvalidOperationException("RegisterEventProcessorAsync call should have failed.");
            });

            Assert.NotNull(ex.InnerException);
            Assert.IsType<MessagingEntityNotFoundException>(ex.InnerException);
        }
    }
}
