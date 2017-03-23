// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Processor.UnitTests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
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

        /// <summary>
        /// While processing events one event causes a failure. Host should be able to recover any error.
        /// </summary>
        /// <returns></returns>
        [Fact]
        async Task HostShouldRecoverWhenProcessEventsAsyncThrows()
        {
            var lastReceivedAt = DateTime.Now;
            var lastReceivedAtLock = new object();
            var poisonMessageReceived = false;
            var poisonMessageProperty = "poison";
            var processorFactory = new TestEventProcessorFactory();
            var receivedEventCounts = new ConcurrentDictionary<string, int>();

            var eventProcessorHost = new EventProcessorHost(
                null,
                PartitionReceiver.DefaultConsumerGroupName,
                this.EventHubConnectionString,
                this.StorageConnectionString,
                this.LeaseContainerName);

            processorFactory.OnCreateProcessor += (f, createArgs) =>
            {
                var processor = createArgs.Item2;
                string partitionId = createArgs.Item1.PartitionId;
                string hostName = createArgs.Item1.Owner;
                string consumerGroupName = createArgs.Item1.ConsumerGroupName;
                processor.OnOpen += (_, partitionContext) => Log($"{hostName} > {consumerGroupName} > Partition {partitionId} TestEventProcessor opened");
                processor.OnClose += (_, closeArgs) => Log($"{hostName} > {consumerGroupName} > Partition {partitionId} TestEventProcessor closing: {closeArgs.Item2}");
                processor.OnProcessError += (_, errorArgs) =>
                {
                    Log($"{hostName} > {consumerGroupName} > Partition {partitionId} TestEventProcessor process error {errorArgs.Item2.Message}");

                    // Throw once more here depending on where we are at exception sequence.
                    if (errorArgs.Item2.Message.Contains("ExceptionSequence1"))
                    {
                        throw new Exception("ExceptionSequence2");
                    }
                };
                processor.OnProcessEvents += (_, eventsArgs) =>
                {
                    int eventCount = eventsArgs.Item2.events != null ? eventsArgs.Item2.events.Count() : 0;
                    Log($"{hostName} > {consumerGroupName} > Partition {partitionId} TestEventProcessor processing {eventCount} event(s)");
                    if (eventCount > 0)
                    {
                        lock (lastReceivedAtLock)
                        {
                            lastReceivedAt = DateTime.Now;
                        }

                        foreach (var e in eventsArgs.Item2.events)
                        {
                            // If this is poisoned event then throw.
                            if (!poisonMessageReceived && e.Properties.ContainsKey(poisonMessageProperty))
                            {
                                poisonMessageReceived = true;
                                Log($"Received poisoned message from partition {partitionId}");
                                throw new Exception("ExceptionSequence1");
                            }

                            // Track received events so we can validate at the end.
                            if (!receivedEventCounts.ContainsKey(partitionId))
                            {
                                receivedEventCounts[partitionId] = 0;
                            }

                            receivedEventCounts[partitionId]++;
                        }
                    }
                };
            };

            try
            {
                Log("Registering processorFactory...");
                var epo = new EventProcessorOptions()
                {
                    MaxBatchSize = 100
                };
                await eventProcessorHost.RegisterEventProcessorFactoryAsync(processorFactory, epo);

                Log("Waiting for partition ownership to settle...");
                await Task.Delay(TimeSpan.FromSeconds(5));

                // Send first set of messages.
                Log("Sending an event to each partition as the first set of messages.");
                var sendTasks = new List<Task>();
                foreach (var partitionId in PartitionIds)
                {
                    sendTasks.Add(this.SendToPartitionAsync(partitionId, $"{partitionId} event.", this.ConnectionStringBuilder.ToString()));
                }
                await Task.WhenAll(sendTasks);

                // Now send 1 poisoned message. This will fail one of the partition pumps.
                Log($"Sending a poison event to partition {PartitionIds.First()}");
                var client = EventHubClient.CreateFromConnectionString(this.EventHubConnectionString);
                var pSender = client.CreatePartitionSender(PartitionIds.First());
                var ed = new EventData(Encoding.UTF8.GetBytes("This is poison message"));
                ed.Properties[poisonMessageProperty] = true;
                await pSender.SendAsync(ed);

                // Wait sometime. The host should fail and then recever during this time.
                await Task.Delay(30000);

                // Send second set of messages.
                Log("Sending an event to each partition as the second set of messages.");
                sendTasks.Clear();
                foreach (var partitionId in PartitionIds)
                {
                    sendTasks.Add(this.SendToPartitionAsync(partitionId, $"{partitionId} event.", this.ConnectionStringBuilder.ToString()));
                }
                await Task.WhenAll(sendTasks);

                Log("Waiting until hosts are idle, i.e. no more messages to receive.");
                while (lastReceivedAt > DateTime.Now.AddSeconds(-60))
                {
                    await Task.Delay(1000);
                }

                Log("Verifying poison message was received");
                Assert.True(poisonMessageReceived, "Didn't receive poison message!");

                Log("Verifying received events by each partition");
                foreach (var partitionId in PartitionIds)
                {
                    if (!receivedEventCounts.ContainsKey(partitionId))
                    {
                        throw new Exception($"Partition {partitionId} didn't receive any messages!");
                    }

                    var receivedEventCount = receivedEventCounts[partitionId];
                    Assert.True(receivedEventCount >= 2, $"Partition {partitionId} received {receivedEventCount} where as at least 2 expected!");
                }
            }
            finally
            {
                Log("Calling UnregisterEventProcessorAsync.");
                await eventProcessorHost.UnregisterEventProcessorAsync();
            }
        }
    }
}
