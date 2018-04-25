// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Tests.Processor
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs.Processor;
    using Xunit;

    public class ProcessorTestBase
    {
        protected string LeaseContainerName;
        protected string[] PartitionIds;

        public ProcessorTestBase()
        {
            // Use entity name as lease container name.
            // Convert to lowercase in case there is capital letter in the entity path.
            // Uppercase is invalid for Azure Storage container names.
            this.LeaseContainerName = new EventHubsConnectionStringBuilder(TestUtility.EventHubsConnectionString).EntityPath.ToLower();

            // Discover partition ids.
            TestUtility.Log("Discovering partitions on eventhub");
            var ehClient = EventHubClient.CreateFromConnectionString(TestUtility.EventHubsConnectionString);
            var eventHubInfo = ehClient.GetRuntimeInformationAsync().Result;
            this.PartitionIds = eventHubInfo.PartitionIds;
            TestUtility.Log($"EventHub has {PartitionIds.Length} partitions");
        }

        /// <summary>
        /// Validating cases where entity path is provided through eventHubPath and EH connection string parameters
        /// on the EPH constructor.
        /// </summary>
        [Fact]
        [DisplayTestMethodName]
        void ProcessorHostEntityPathSetting()
        {
            var csb = new EventHubsConnectionStringBuilder(TestUtility.EventHubsConnectionString)
            {
                EntityPath = "myeh"
            };

            // Entity path provided in the connection string.
            TestUtility.Log("Testing condition: Entity path provided in the connection string only.");
            var eventProcessorHost = new EventProcessorHost(
                null,
                PartitionReceiver.DefaultConsumerGroupName,
                csb.ToString(),
                TestUtility.StorageConnectionString,
                this.LeaseContainerName);
            Assert.Equal("myeh", eventProcessorHost.EventHubPath);

            // Entity path provided in the eventHubPath parameter.
            TestUtility.Log("Testing condition: Entity path provided in the eventHubPath only.");
            csb.EntityPath = null;
            eventProcessorHost = new EventProcessorHost(
                "myeh2",
                PartitionReceiver.DefaultConsumerGroupName,
                csb.ToString(),
                TestUtility.StorageConnectionString,
                this.LeaseContainerName);
            Assert.Equal("myeh2", eventProcessorHost.EventHubPath);

            // The same entity path provided in both eventHubPath parameter and the connection string.
            TestUtility.Log("Testing condition: The same entity path provided in the eventHubPath and connection string.");
            csb.EntityPath = "mYeH";
            eventProcessorHost = new EventProcessorHost(
                "myeh",
                PartitionReceiver.DefaultConsumerGroupName,
                csb.ToString(),
                TestUtility.StorageConnectionString,
                this.LeaseContainerName);
            Assert.Equal("myeh", eventProcessorHost.EventHubPath);

            // Entity path not provided in both eventHubPath and the connection string.
            TestUtility.Log("Testing condition: Entity path not provided in both eventHubPath and connection string.");
            try
            {
                csb.EntityPath = null;
                new EventProcessorHost(
                    string.Empty,
                    PartitionReceiver.DefaultConsumerGroupName,
                    csb.ToString(),
                    TestUtility.StorageConnectionString,
                    this.LeaseContainerName);
                throw new Exception("Entity path wasn't provided and this new call was supposed to fail");
            }
            catch (ArgumentException)
            {
                TestUtility.Log("Caught ArgumentException as expected.");
            }

            // Entity path conflict.
            TestUtility.Log("Testing condition: Entity path conflict.");
            try
            {
                csb.EntityPath = "myeh";
                new EventProcessorHost(
                    "myeh2",
                    PartitionReceiver.DefaultConsumerGroupName,
                    csb.ToString(),
                    TestUtility.StorageConnectionString,
                    this.LeaseContainerName);
                throw new Exception("Entity path values conflict and this new call was supposed to fail");
            }
            catch (ArgumentException)
            {
                TestUtility.Log("Caught ArgumentException as expected.");
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task SingleProcessorHost()
        {
            var epo = await GetOptionsAsync();

            var eventProcessorHost = new EventProcessorHost(
                null,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                Guid.NewGuid().ToString());

            await RunGenericScenario(eventProcessorHost, epo);
        }

        [Fact]
        [DisplayTestMethodName]
        async Task MultipleProcessorHosts()
        {
            int hostCount = 3;

            TestUtility.Log($"Testing with {hostCount} EventProcessorHost instances");

            // Prepare partition trackers.
            var partitionReceiveEvents = new ConcurrentDictionary<string, AsyncAutoResetEvent>();
            foreach (var partitionId in PartitionIds)
            {
                partitionReceiveEvents[partitionId] = new AsyncAutoResetEvent(false);
            }

            // Prepare host trackers.
            var hostReceiveEvents = new ConcurrentDictionary<string, AsyncAutoResetEvent>();

            var hosts = new List<EventProcessorHost>();
            try
            {
                for (int hostId = 0; hostId < hostCount; hostId++)
                {
                    var thisHostName = $"host-{hostId}";
                    hostReceiveEvents[thisHostName] = new AsyncAutoResetEvent(false);

                    TestUtility.Log("Creating EventProcessorHost");
                    var eventProcessorHost = new EventProcessorHost(
                        thisHostName,
                        string.Empty, // Passing empty as entity path here rsince path is already in EH connection string.
                        PartitionReceiver.DefaultConsumerGroupName,
                        TestUtility.EventHubsConnectionString,
                        TestUtility.StorageConnectionString,
                        Guid.NewGuid().ToString());
                    hosts.Add(eventProcessorHost);
                    TestUtility.Log($"Calling RegisterEventProcessorAsync");
                    var processorOptions = new EventProcessorOptions
                    {
                        ReceiveTimeout = TimeSpan.FromSeconds(10),
                        InvokeProcessorAfterReceiveTimeout = true,
                        MaxBatchSize = 100,
                        InitialOffsetProvider = pId => EventPosition.FromEnqueuedTime(DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(60)))
                    };

                    var processorFactory = new TestEventProcessorFactory();
                    processorFactory.OnCreateProcessor += (f, createArgs) =>
                    {
                        var processor = createArgs.Item2;
                        string partitionId = createArgs.Item1.PartitionId;
                        string hostName = createArgs.Item1.Owner;
                        processor.OnOpen += (_, partitionContext) => TestUtility.Log($"{hostName} > Partition {partitionId} TestEventProcessor opened");
                        processor.OnClose += (_, closeArgs) => TestUtility.Log($"{hostName} > Partition {partitionId} TestEventProcessor closing: {closeArgs.Item2}");
                        processor.OnProcessError += (_, errorArgs) => TestUtility.Log($"{hostName} > Partition {partitionId} TestEventProcessor process error {errorArgs.Item2.Message}");
                        processor.OnProcessEvents += (_, eventsArgs) =>
                        {
                            int eventCount = eventsArgs.Item2.events != null ? eventsArgs.Item2.events.Count() : 0;
                            if (eventCount > 0)
                            {
                                TestUtility.Log($"{hostName} > Partition {partitionId} TestEventProcessor processing {eventCount} event(s)");
                                partitionReceiveEvents[partitionId].Set();
                                hostReceiveEvents[hostName].Set();
                            }
                        };
                    };

                    await eventProcessorHost.RegisterEventProcessorFactoryAsync(processorFactory, processorOptions);
                }

                // Allow some time for each host to own at least 1 partition.
                // Partition stealing logic balances partition ownership one at a time.
                TestUtility.Log("Waiting for partition ownership to settle...");
                await Task.Delay(TimeSpan.FromSeconds(60));

                TestUtility.Log("Sending an event to each partition");
                var ehClient = EventHubClient.CreateFromConnectionString(TestUtility.EventHubsConnectionString);
                var sendTasks = new List<Task>();
                foreach (var partitionId in PartitionIds)
                {
                    sendTasks.Add(TestUtility.SendToPartitionAsync(ehClient, partitionId, $"{partitionId} event."));
                }
                await Task.WhenAll(sendTasks);

                TestUtility.Log("Verifying an event was received by each partition");
                foreach (var e in partitionReceiveEvents)
                {
                    bool ret = await e.Value.WaitAsync(TimeSpan.FromSeconds(30));
                    Assert.True(ret, $"Partition {e.Key} didn't receive any message!");
                }

                TestUtility.Log("Verifying at least an event was received by each host");
                foreach (var e in hostReceiveEvents)
                {
                    bool ret = await e.Value.WaitAsync(TimeSpan.FromSeconds(30));
                    Assert.True(ret, $"Host {e.Key} didn't receive any message!");
                }
            }
            finally
            {
                var shutdownTasks = new List<Task>();
                foreach (var host in hosts)
                {
                    TestUtility.Log($"Host {host} Calling UnregisterEventProcessorAsync.");
                    shutdownTasks.Add(host.UnregisterEventProcessorAsync());
                }

                await Task.WhenAll(shutdownTasks);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task WithBlobPrefix()
        {
            string leaseContainerName = Guid.NewGuid().ToString();

            var epo = await GetOptionsAsync();

            // Consume all messages with first host.
            // Create host with 'firsthost' prefix.
            var eventProcessorHostFirst = new EventProcessorHost(
                "host1",
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                leaseContainerName,
                "firsthost");
            var runResult1 = await RunGenericScenario(eventProcessorHostFirst, epo);

            // Consume all messages with second host.
            // Create host with 'secondhost' prefix.
            // Although on the same lease container, this second host should receive exactly the same set of messages
            // as the first host.
            var eventProcessorHostSecond = new EventProcessorHost(
                "host2",
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                leaseContainerName,
                "secondhost");
            var runResult2 = await RunGenericScenario(eventProcessorHostSecond, epo, totalNumberOfEventsToSend: 0);

            // Confirm that we are looking at 2 identical sets of messages in the end.
            foreach (var kvp in runResult1.ReceivedEvents)
            {
                Assert.True(kvp.Value.Count() == runResult2.ReceivedEvents[kvp.Key].Count,
                    $"The sets of messages returned from first host and the second host are different for partition {kvp.Key}.");
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task InvokeAfterReceiveTimeoutTrue()
        {
            const int ReceiveTimeoutInSeconds = 15;

            TestUtility.Log("Testing EventProcessorHost with InvokeProcessorAfterReceiveTimeout=true");

            var emptyBatchReceiveEvents = new ConcurrentDictionary<string, AsyncAutoResetEvent>();
            foreach (var partitionId in PartitionIds)
            {
                emptyBatchReceiveEvents[partitionId] = new AsyncAutoResetEvent(false);
            }

            var eventProcessorHost = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                Guid.NewGuid().ToString());

            var processorOptions = new EventProcessorOptions
            {
                ReceiveTimeout = TimeSpan.FromSeconds(ReceiveTimeoutInSeconds),
                InvokeProcessorAfterReceiveTimeout = true,
                InitialOffsetProvider = pId => EventPosition.FromEnd()
            };

            var processorFactory = new TestEventProcessorFactory();
            processorFactory.OnCreateProcessor += (f, createArgs) =>
            {
                var processor = createArgs.Item2;
                string partitionId = createArgs.Item1.PartitionId;
                processor.OnOpen += (_, partitionContext) => TestUtility.Log($"Partition {partitionId} TestEventProcessor opened");
                processor.OnProcessEvents += (_, eventsArgs) =>
                {
                    int eventCount = eventsArgs.Item2.events != null ? eventsArgs.Item2.events.Count() : 0;
                    TestUtility.Log($"Partition {partitionId} TestEventProcessor processing {eventCount} event(s)");
                    if (eventCount == 0)
                    {
                        var emptyBatchReceiveEvent = emptyBatchReceiveEvents[partitionId];
                        emptyBatchReceiveEvent.Set();
                    }
                };
            };

            await eventProcessorHost.RegisterEventProcessorFactoryAsync(processorFactory, processorOptions);
            try
            {
                TestUtility.Log("Waiting for each partition to receive an empty batch of events...");
                foreach (var partitionId in PartitionIds)
                {
                    var emptyBatchReceiveEvent = emptyBatchReceiveEvents[partitionId];
                    bool emptyBatchReceived = await emptyBatchReceiveEvent.WaitAsync(TimeSpan.FromSeconds(ReceiveTimeoutInSeconds * 2));
                    Assert.True(emptyBatchReceived, $"Partition {partitionId} didn't receive an empty batch!");
                }
            }
            finally
            {
                TestUtility.Log("Calling UnregisterEventProcessorAsync");
                await eventProcessorHost.UnregisterEventProcessorAsync();
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task InvokeAfterReceiveTimeoutFalse()
        {
            const int ReceiveTimeoutInSeconds = 15;

            TestUtility.Log("Calling RegisterEventProcessorAsync with InvokeProcessorAfterReceiveTimeout=false");

            var eventProcessorHost = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                this.LeaseContainerName);

            var processorOptions = new EventProcessorOptions
            {
                ReceiveTimeout = TimeSpan.FromSeconds(ReceiveTimeoutInSeconds),
                InvokeProcessorAfterReceiveTimeout = false,
                MaxBatchSize = 100
            };

            var emptyBatchReceiveEvent = new AsyncAutoResetEvent(false);
            var processorFactory = new TestEventProcessorFactory();
            processorFactory.OnCreateProcessor += (f, createArgs) =>
            {
                var processor = createArgs.Item2;
                string partitionId = createArgs.Item1.PartitionId;
                processor.OnProcessEvents += (_, eventsArgs) =>
                {
                    int eventCount = eventsArgs.Item2.events != null ? eventsArgs.Item2.events.Count() : 0;
                    TestUtility.Log($"Partition {partitionId} TestEventProcessor processing {eventCount} event(s)");
                    if (eventCount == 0)
                    {
                        emptyBatchReceiveEvent.Set();
                    }
                };
            };

            await eventProcessorHost.RegisterEventProcessorFactoryAsync(processorFactory, processorOptions);
            try
            {
                TestUtility.Log("Verifying no empty batches arrive...");
                bool waitSucceeded = await emptyBatchReceiveEvent.WaitAsync(TimeSpan.FromSeconds(ReceiveTimeoutInSeconds * 2));
                Assert.False(waitSucceeded, "No empty batch should have been received!");
            }
            finally
            {
                TestUtility.Log("Calling UnregisterEventProcessorAsync");
                await eventProcessorHost.UnregisterEventProcessorAsync();
            }
        }

        /// <summary>
        /// This test requires a eventhub with consumer groups $Default and cgroup1.
        /// </summary>
        /// <returns></returns>
        [Fact]
        [DisplayTestMethodName]
        async Task MultipleConsumerGroups()
        {
            var customConsumerGroupName = "cgroup1";

            var ehClient = EventHubClient.CreateFromConnectionString(TestUtility.EventHubsConnectionString);

            // Generate a new lease container name that will be used through out the test.
            string leaseContainerName = Guid.NewGuid().ToString();

            var consumerGroupNames = new[]  { PartitionReceiver.DefaultConsumerGroupName, customConsumerGroupName };
            var processorOptions = new EventProcessorOptions
            {
                ReceiveTimeout = TimeSpan.FromSeconds(15),
                InitialOffsetProvider = pId => EventPosition.FromEnd()
            };
            var processorFactory = new TestEventProcessorFactory();
            var partitionReceiveEvents = new ConcurrentDictionary<string, AsyncAutoResetEvent>();
            var hosts = new List<EventProcessorHost>();

            // Confirm that custom consumer group exists before starting hosts.
            try
            {
                // Create a receiver on the consumer group and try to receive.
                // Receive call will fail if consumer group is missing.
                var receiver = ehClient.CreateReceiver(customConsumerGroupName, this.PartitionIds.First(), EventPosition.FromStart());
                await receiver.ReceiveAsync(1, TimeSpan.FromSeconds(5));
            }
            catch (MessagingEntityNotFoundException)
            {
                throw new Exception($"Cunsumer group {customConsumerGroupName} cannot be found. MultipleConsumerGroups unit test requires consumer group '{customConsumerGroupName}' to be created before running the test.");
            }

            processorFactory.OnCreateProcessor += (f, createArgs) =>
            {
                var processor = createArgs.Item2;
                string partitionId = createArgs.Item1.PartitionId;
                string hostName = createArgs.Item1.Owner;
                string consumerGroupName = createArgs.Item1.ConsumerGroupName;
                processor.OnOpen += (_, partitionContext) => TestUtility.Log($"{hostName} > {consumerGroupName} > Partition {partitionId} TestEventProcessor opened");
                processor.OnClose += (_, closeArgs) => TestUtility.Log($"{hostName} > {consumerGroupName} > Partition {partitionId} TestEventProcessor closing: {closeArgs.Item2}");
                processor.OnProcessError += (_, errorArgs) => TestUtility.Log($"{hostName} > {consumerGroupName} > Partition {partitionId} TestEventProcessor process error {errorArgs.Item2.Message}");
                processor.OnProcessEvents += (_, eventsArgs) =>
                {
                    int eventCount = eventsArgs.Item2.events != null ? eventsArgs.Item2.events.Count() : 0;
                    TestUtility.Log($"{hostName} > {consumerGroupName} > Partition {partitionId} TestEventProcessor processing {eventCount} event(s)");
                    if (eventCount > 0)
                    {
                        var receivedEvent = partitionReceiveEvents[consumerGroupName + "-" + partitionId];
                        receivedEvent.Set();
                    }
                };
            };

            try
            {
                // Register a new host for each consumer group.
                foreach (var consumerGroupName in consumerGroupNames)
                {
                    var eventProcessorHost = new EventProcessorHost(
                        string.Empty,
                        consumerGroupName,
                        TestUtility.EventHubsConnectionString,
                        TestUtility.StorageConnectionString,
                        leaseContainerName);

                    TestUtility.Log($"Calling RegisterEventProcessorAsync on consumer group {consumerGroupName}");

                    foreach (var partitionId in PartitionIds)
                    {
                        partitionReceiveEvents[consumerGroupName + "-" + partitionId] = new AsyncAutoResetEvent(false);
                    }

                    await eventProcessorHost.RegisterEventProcessorFactoryAsync(processorFactory, processorOptions);
                    hosts.Add(eventProcessorHost);
                }

                await Task.Delay(10000);
                TestUtility.Log("Sending an event to each partition");
                var sendTasks = new List<Task>();
                foreach (var partitionId in PartitionIds)
                {
                    sendTasks.Add(TestUtility.SendToPartitionAsync(ehClient, partitionId, $"{partitionId} event."));
                }

                await Task.WhenAll(sendTasks);

                TestUtility.Log("Verifying an event was received by each partition for each consumer group");
                foreach (var consumerGroupName in consumerGroupNames)
                {
                    foreach (var partitionId in PartitionIds)
                    {
                        var receivedEvent = partitionReceiveEvents[consumerGroupName + "-" + partitionId];
                        bool partitionReceivedMessage = await receivedEvent.WaitAsync(TimeSpan.FromSeconds(60));
                        Assert.True(partitionReceivedMessage, $"ConsumerGroup {consumerGroupName} > Partition {partitionId} didn't receive any message!");
                    }
                }

                TestUtility.Log("Success");
            }
            finally
            {
                TestUtility.Log("Calling UnregisterEventProcessorAsync on both hosts.");
                foreach (var eph in hosts)
                {
                    await eph.UnregisterEventProcessorAsync();
                }
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task InitialOffsetProviderWithDateTime()
        {
            // Send and receive single message so we can find out enqueue date-time of the last message.
            var partitions = await DiscoverEndOfStream();

            // We will use last enqueued message's enqueue date-time so EPH will pick messages only after that point.
            var lastEnqueueDateTime = partitions.Max(le => le.Value.Item2);
            TestUtility.Log($"Last message enqueued at {lastEnqueueDateTime}");

            // Use a randomly generated container name so that initial offset provider will be respected.
            var eventProcessorHost = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                Guid.NewGuid().ToString());

            var processorOptions = new EventProcessorOptions
            {
                ReceiveTimeout = TimeSpan.FromSeconds(15),
                InitialOffsetProvider = partitionId => EventPosition.FromEnqueuedTime(lastEnqueueDateTime),
                MaxBatchSize = 100
            };

            var runResult = await this.RunGenericScenario(eventProcessorHost, processorOptions);

            // We should have received only 1 event from each partition.
            Assert.False(runResult.ReceivedEvents.Any(kvp => kvp.Value.Count != 1), "One of the partitions didn't return exactly 1 event");
        }

        [Fact]
        [DisplayTestMethodName]
        async Task InitialOffsetProviderWithOffset()
        {
            // Send and receive single message so we can find out offset of the last message.
            var partitions = await DiscoverEndOfStream();
            TestUtility.Log("Discovered last event offsets on each partition as below:");
            foreach (var p in partitions)
            {
                TestUtility.Log($"Partition {p.Key}: {p.Value.Item1}");
            }

            // Use a randomly generated container name so that initial offset provider will be respected.
            var eventProcessorHost = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                Guid.NewGuid().ToString());

            var processorOptions = new EventProcessorOptions
            {
                ReceiveTimeout = TimeSpan.FromSeconds(15),
                InitialOffsetProvider = partitionId => EventPosition.FromOffset(partitions[partitionId].Item1),
                MaxBatchSize = 100
            };

            var runResult = await this.RunGenericScenario(eventProcessorHost, processorOptions);

            // We should have received only 1 event from each partition.
            Assert.False(runResult.ReceivedEvents.Any(kvp => kvp.Value.Count != 1), "One of the partitions didn't return exactly 1 event");
        }

        [Fact]
        [DisplayTestMethodName]
        async Task InitialOffsetProviderWithEndOfStream()
        {
            // Use a randomly generated container name so that initial offset provider will be respected.
            var eventProcessorHost = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                Guid.NewGuid().ToString());

            var processorOptions = new EventProcessorOptions
            {
                ReceiveTimeout = TimeSpan.FromSeconds(15),
                InitialOffsetProvider = partitionId => EventPosition.FromEnd(),
                MaxBatchSize = 100
            };

            var runResult = await this.RunGenericScenario(eventProcessorHost, processorOptions);

            // We should have received only 1 event from each partition.
            Assert.False(runResult.ReceivedEvents.Any(kvp => kvp.Value.Count != 1), "One of the partitions didn't return exactly 1 event");
        }

        [Fact]
        [DisplayTestMethodName]
        async Task InitialOffsetProviderOverrideBehavior()
        {
            // Generate a new lease container name that will be used through out the test.
            string leaseContainerName = Guid.NewGuid().ToString();
            TestUtility.Log($"Using lease container {leaseContainerName}");

            var epo = await GetOptionsAsync();

            // First host will send and receive as usual.
            var eventProcessorHost = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                leaseContainerName);
            await this.RunGenericScenario(eventProcessorHost, epo);

            // Second host will use an initial offset provider.
            // Since we are still on the same lease container, initial offset provider shouldn't rule.
            // We should continue receiving where we left instead if start-of-stream where initial offset provider dictates.
            eventProcessorHost = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                leaseContainerName);
            var processorOptions = new EventProcessorOptions
            {
                ReceiveTimeout = TimeSpan.FromSeconds(15),
                InitialOffsetProvider = partitionId => EventPosition.FromStart(),
                MaxBatchSize = 100
            };

            var runResult = await this.RunGenericScenario(eventProcessorHost, processorOptions, checkpointLastEvent: false);

            // We should have received only 1 event from each partition.
            Assert.False(runResult.ReceivedEvents.Any(kvp => kvp.Value.Count != 1), "One of the partitions didn't return exactly 1 event");
        }

        [Fact]
        [DisplayTestMethodName]
        async Task CheckpointEventDataShouldHold()
        {
            // Generate a new lease container name that will use through out the test.
            string leaseContainerName = Guid.NewGuid().ToString();

            var epo = await GetOptionsAsync();

            // Consume all messages with first host.
            var eventProcessorHostFirst = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                leaseContainerName);
            await RunGenericScenario(eventProcessorHostFirst, epo);

            // For the second time we initiate a host and this time it should pick from where the previous host left.
            // In other words, it shouldn't start receiving from start of the stream.
            var eventProcessorHostSecond = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                leaseContainerName);
            var runResult = await RunGenericScenario(eventProcessorHostSecond);

            // We should have received only 1 event from each partition.
            Assert.False(runResult.ReceivedEvents.Any(kvp => kvp.Value.Count != 1), "One of the partitions didn't return exactly 1 event");
        }

        [Fact]
        [DisplayTestMethodName]
        async Task CheckpointBatchShouldHold()
        {
            // Generate a new lease container name that will use through out the test.
            string leaseContainerName = Guid.NewGuid().ToString();

            var epo = await GetOptionsAsync();

            // Consume all messages with first host.
            var eventProcessorHostFirst = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                leaseContainerName);
            await RunGenericScenario(eventProcessorHostFirst, epo, checkpointLastEvent: false, checkpointBatch: true);

            // For the second time we initiate a host and this time it should pick from where the previous host left.
            // In other words, it shouldn't start receiving from start of the stream.
            var eventProcessorHostSecond = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                leaseContainerName);
            var runResult = await RunGenericScenario(eventProcessorHostSecond, epo);

            // We should have received only 1 event from each partition.
            Assert.False(runResult.ReceivedEvents.Any(kvp => kvp.Value.Count != 1), "One of the partitions didn't return exactly 1 event");
        }

        [Fact]
        [DisplayTestMethodName]
        async Task HostShouldRecoverAfterReceiverDisconnection()
        {
            // We will target one partition and do validation on it.
            var targetPartition = this.PartitionIds.First();

            int targetPartitionOpens = 0;
            int targetPartitionCloses = 0;
            int targetPartitionErrors = 0;
            PartitionReceiver externalReceiver = null;

            var eventProcessorHost = new EventProcessorHost(
                "ephhost",
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                Guid.NewGuid().ToString());

            try
            {
                var processorFactory = new TestEventProcessorFactory();

                processorFactory.OnCreateProcessor += (f, createArgs) =>
                {
                    var processor = createArgs.Item2;
                    string partitionId = createArgs.Item1.PartitionId;
                    string hostName = createArgs.Item1.Owner;
                    processor.OnOpen += (_, partitionContext) =>
                        {
                            TestUtility.Log($"{hostName} > Partition {partitionId} TestEventProcessor opened");
                            if (partitionId == targetPartition)
                            {
                                Interlocked.Increment(ref targetPartitionOpens);
                            }
                        };
                    processor.OnClose += (_, closeArgs) =>
                        {
                            TestUtility.Log($"{hostName} > Partition {partitionId} TestEventProcessor closing: {closeArgs.Item2}");
                            if (partitionId == targetPartition && closeArgs.Item2 == CloseReason.Shutdown)
                            {
                                Interlocked.Increment(ref targetPartitionCloses);
                            }
                        };
                    processor.OnProcessError += (_, errorArgs) =>
                        {
                            TestUtility.Log($"{hostName} > Partition {partitionId} TestEventProcessor process error {errorArgs.Item2.Message}");
                            if (partitionId == targetPartition && errorArgs.Item2 is ReceiverDisconnectedException)
                            {
                                Interlocked.Increment(ref targetPartitionErrors);
                            }
                        };
                };

                await eventProcessorHost.RegisterEventProcessorFactoryAsync(processorFactory);

                // Wait 15 seconds then create a new epoch receiver.
                // This will trigger ReceiverDisconnectedExcetion in the host.
                await Task.Delay(15000);

                TestUtility.Log("Creating a new receiver with epoch 2. This will trigger ReceiverDisconnectedException in the host.");
                var ehClient = EventHubClient.CreateFromConnectionString(TestUtility.EventHubsConnectionString);
                externalReceiver = ehClient.CreateEpochReceiver(PartitionReceiver.DefaultConsumerGroupName,
                    targetPartition, EventPosition.FromStart(), 2);
                await externalReceiver.ReceiveAsync(100, TimeSpan.FromSeconds(5));

                // Give another 1 minute for host to recover then do the validatins.
                await Task.Delay(60000);

                TestUtility.Log("Verifying that host was able to receive ReceiverDisconnectedException");
                Assert.True(targetPartitionErrors == 1, $"Host received {targetPartitionErrors} ReceiverDisconnectedExceptions!");

                TestUtility.Log("Verifying that host was able to reopen the partition");
                Assert.True(targetPartitionOpens == 2, $"Host opened target partition {targetPartitionOpens} times!");

                TestUtility.Log("Verifying that host notified by close");
                Assert.True(targetPartitionCloses == 1, $"Host closed target partition {targetPartitionCloses} times!");
            }
            finally
            {
                TestUtility.Log("Calling UnregisterEventProcessorAsync");
                await eventProcessorHost.UnregisterEventProcessorAsync();

                if (externalReceiver != null)
                {
                    await externalReceiver.CloseAsync();
                }
            }
        }

        /// <summary>
        /// If a host doesn't checkpoint on the processed events and shuts down, new host should start processing from the beginning.
        /// </summary>
        /// <returns></returns>
        [Fact]
        [DisplayTestMethodName]
        async Task NoCheckpointThenNewHostReadsFromStart()
        {
            // Generate a new lease container name that will be used through out the test.
            string leaseContainerName = Guid.NewGuid().ToString();

            var epo = await GetOptionsAsync();

            // Consume all messages with first host.
            var eventProcessorHostFirst = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                leaseContainerName);
            var runResult1 = await RunGenericScenario(eventProcessorHostFirst, epo, checkpointLastEvent: false);
            var totalEventsFromFirstHost = runResult1.ReceivedEvents.Sum(part => part.Value.Count);

            // Second time we initiate a host, it should receive exactly the same number of evets.
            var eventProcessorHostSecond = new EventProcessorHost(
                string.Empty,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                leaseContainerName);
            var runResult2 = await RunGenericScenario(eventProcessorHostSecond, epo, 0);
            var totalEventsFromSecondHost = runResult2.ReceivedEvents.Sum(part => part.Value.Count);

            // Second host should have received the same number of events as the first host.
            Assert.True(totalEventsFromFirstHost == totalEventsFromSecondHost,
                $"Second host received {totalEventsFromSecondHost} events where as first host receive {totalEventsFromFirstHost} events.");
        }

        /// <summary>
        /// Checkpointing every message received should be Ok. No failures expected.
        /// </summary>
        /// <returns></returns>
        [Fact]
        [DisplayTestMethodName]
        async Task CheckpointEveryMessageReceived()
        {
            var epo = await GetOptionsAsync();

            var eventProcessorHost = new EventProcessorHost(
                null,
                PartitionReceiver.DefaultConsumerGroupName,
                TestUtility.EventHubsConnectionString,
                TestUtility.StorageConnectionString,
                Guid.NewGuid().ToString());

            var runResult = await RunGenericScenario(eventProcessorHost, epo, totalNumberOfEventsToSend: 10,
                checkpointLastEvent: false, checkpoingEveryEvent: true);

            // Validate there were not failures.
            Assert.True(runResult.NumberOfFailures == 0, $"RunResult returned with {runResult.NumberOfFailures} failures!");
        }

        async Task<Dictionary<string, Tuple<string, DateTime>>> DiscoverEndOfStream()
        {
            var ehClient = EventHubClient.CreateFromConnectionString(TestUtility.EventHubsConnectionString);
            var partitions = new Dictionary<string, Tuple<string, DateTime>>();

            foreach (var pid in this.PartitionIds)
            {
                var pInfo = await ehClient.GetPartitionRuntimeInformationAsync(pid);
                partitions.Add(pid, Tuple.Create(pInfo.LastEnqueuedOffset, pInfo.LastEnqueuedTimeUtc));
            }

            return partitions.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }

        async Task<GenericScenarioResult> RunGenericScenario(EventProcessorHost eventProcessorHost,
            EventProcessorOptions epo = null, int totalNumberOfEventsToSend = 1, bool checkpointLastEvent = true,
            bool checkpointBatch = false, bool checkpoingEveryEvent = false)
        {
            var runResult = new GenericScenarioResult();
            var lastReceivedAt = DateTime.Now;

            if (epo == null)
            {
                epo = new EventProcessorOptions
                {
                    ReceiveTimeout = TimeSpan.FromSeconds(15),
                    MaxBatchSize = 100
                };
            }

            epo.SetExceptionHandler(TestEventProcessorFactory.ErrorNotificationHandler);

            try
            {
                TestUtility.Log($"Calling RegisterEventProcessorAsync");
                var processorFactory = new TestEventProcessorFactory();

                processorFactory.OnCreateProcessor += (f, createArgs) =>
                {
                    var processor = createArgs.Item2;
                    string partitionId = createArgs.Item1.PartitionId;
                    string hostName = createArgs.Item1.Owner;
                    processor.OnOpen += (_, partitionContext) => TestUtility.Log($"{hostName} > Partition {partitionId} TestEventProcessor opened");
                    processor.OnClose += (_, closeArgs) => TestUtility.Log($"{hostName} > Partition {partitionId} TestEventProcessor closing: {closeArgs.Item2}");
                    processor.OnProcessError += (_, errorArgs) =>
                        {
                            TestUtility.Log($"{hostName} > Partition {partitionId} TestEventProcessor process error {errorArgs.Item2.Message}");
                            Interlocked.Increment(ref runResult.NumberOfFailures);
                        };
                    processor.OnProcessEvents += (_, eventsArgs) =>
                    {
                        int eventCount = eventsArgs.Item2.events != null ? eventsArgs.Item2.events.Count() : 0;
                        TestUtility.Log($"{hostName} > Partition {partitionId} TestEventProcessor processing {eventCount} event(s)");
                        if (eventCount > 0)
                        {
                            lastReceivedAt = DateTime.Now;
                            runResult.AddEvents(partitionId, eventsArgs.Item2.events);

                            foreach (var e in eventsArgs.Item2.events)
                            {
                                // Checkpoint every event received?
                                if (checkpoingEveryEvent)
                                {
                                    eventsArgs.Item1.CheckpointAsync(e).Wait();
                                }
                            }
                        }

                        eventsArgs.Item2.checkPointLastEvent = checkpointLastEvent;
                        eventsArgs.Item2.checkPointBatch = checkpointBatch;
                    };
                };

                await eventProcessorHost.RegisterEventProcessorFactoryAsync(processorFactory, epo);

                // Wait 5 seconds to avoid races in scenarios like EndOfStream.
                await Task.Delay(5000);

                if (totalNumberOfEventsToSend > 0)
                {
                    TestUtility.Log($"Sending {totalNumberOfEventsToSend} event(s) to each partition");
                    var ehClient = EventHubClient.CreateFromConnectionString(TestUtility.EventHubsConnectionString);
                    var sendTasks = new List<Task>();
                    foreach (var partitionId in PartitionIds)
                    {
                        sendTasks.Add(TestUtility.SendToPartitionAsync(ehClient, partitionId, $"{partitionId} event.", totalNumberOfEventsToSend));
                    }
                    await Task.WhenAll(sendTasks);
                }

                // Wait until all partitions are silent, i.e. no more events to receive.
                while (lastReceivedAt > DateTime.Now.AddSeconds(-30))
                {
                    await Task.Delay(1000);
                }

                TestUtility.Log($"Verifying at least {totalNumberOfEventsToSend} event(s) was received by each partition");
                foreach (var partitionId in PartitionIds)
                {
                    Assert.True(runResult.ReceivedEvents.ContainsKey(partitionId),
                        $"Partition {partitionId} didn't receive any messages. Expected {totalNumberOfEventsToSend}, received 0.");
                    Assert.True(runResult.ReceivedEvents[partitionId].Count >= totalNumberOfEventsToSend,
                        $"Partition {partitionId} didn't receive expected number of messages. Expected {totalNumberOfEventsToSend}, received {runResult.ReceivedEvents[partitionId].Count}.");
                }

                TestUtility.Log("Success");
            }
            finally
            {
                TestUtility.Log("Calling UnregisterEventProcessorAsync");
                await eventProcessorHost.UnregisterEventProcessorAsync();
            }

            return runResult;
        }

        async Task<EventProcessorOptions> GetOptionsAsync()
        {
            var partitions = await DiscoverEndOfStream();
            return new EventProcessorOptions()
            {
                MaxBatchSize = 100,
                InitialOffsetProvider = pId => EventPosition.FromOffset(partitions[pId].Item1)
            };
        }
    }

    class GenericScenarioResult
    {
        public ConcurrentDictionary<string, List<EventData>> ReceivedEvents = new ConcurrentDictionary<string, List<EventData>>();
        public int NumberOfFailures = 0;

        object listLock = new object();

        public void AddEvents(string partitionId, IEnumerable<EventData> addEvents)
        {
            List<EventData> events;
            this.ReceivedEvents.TryGetValue(partitionId, out events);
            if (events == null)
            {
                events = new List<EventData>();
            }

            // Account the case where 2 hosts racing by working on the same partition.
            lock (listLock)
            {
                events.AddRange(addEvents);
            }

            this.ReceivedEvents[partitionId] = events;
        }
    }
}

