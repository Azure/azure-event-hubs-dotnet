using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.Azure.EventHubs.ServiceFabricProcessor;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Xunit;

namespace Microsoft.Azure.EventHubs.Tests.ServiceFabricProcessor
{
    public class OptionsTests
    {
        [Fact]
        [DisplayTestMethodName]
        void SimpleOptionsTest()
        {
            TestState state = new TestState();
            state.Initialize("SimpleOptions", 1, 0);
            const int testBatchSize = 42;
            Assert.False(state.Options.MaxBatchSize == testBatchSize); // make sure new value is not the same as the default
            state.Options.MaxBatchSize = testBatchSize;
            const int testPrefetchCount = 444;
            Assert.False(state.Options.PrefetchCount == testPrefetchCount);
            state.Options.PrefetchCount = testPrefetchCount;
            TimeSpan testReceiveTimeout = TimeSpan.FromSeconds(10.0);
            Assert.False(state.Options.ReceiveTimeout.Equals(testReceiveTimeout));
            state.Options.ReceiveTimeout = testReceiveTimeout;
            ReceiverOptions receiverOptions = new ReceiverOptions();
            Assert.Null(receiverOptions.Identifier);
            const string tag = "SimpleOptions";
            receiverOptions.Identifier = tag;
            state.Options.ClientReceiverOptions = receiverOptions;

            Microsoft.Azure.EventHubs.ServiceFabricProcessor.ServiceFabricProcessor sfp =
                new Microsoft.Azure.EventHubs.ServiceFabricProcessor.ServiceFabricProcessor(
                    state.ServiceUri,
                    state.ServicePartitionId,
                    state.StateManager,
                    state.StatefulServicePartition,
                    state.Processor,
                    state.ConnectionString,
                    "$Default",
                    state.Options);
            sfp.MockMode = state.PartitionLister;
            sfp.EventHubClientFactory = new EventHubMocks.EventHubClientFactoryMock(1, tag);

            state.PrepareToRun();
            state.StartRun(sfp);

            state.VerifyNormalStartup(10);
            state.CountNBatches(5, 10);

            Assert.True(EventHubMocks.PartitionReceiverMock.receivers.ContainsKey(tag), "Cannot find receiver");
            EventHubMocks.PartitionReceiverMock testReceiver = EventHubMocks.PartitionReceiverMock.receivers[tag];
            Assert.True(testReceiver.HandlerBatchSize == testBatchSize, $"Unexpected batch size {testReceiver.HandlerBatchSize}");
            Assert.True(testReceiver.PrefetchCount == testPrefetchCount, $"Unexpected prefetch count {testReceiver.PrefetchCount}");
            Assert.True(testReceiver.ReceiveTimeout.Equals(testReceiveTimeout),
                $"Unexpected receive timeout {testReceiver.ReceiveTimeout}");
            Assert.NotNull(testReceiver.Options);
            Assert.Equal(testReceiver.Options.Identifier, tag);

            // Check that RuntimeInformation was not updated
            Assert.True(state.Processor.LatestContext.RuntimeInformation.LastSequenceNumber == 0L,
                $"RuntimeInformation.LastSequenceNumber is {state.Processor.LatestContext.RuntimeInformation.LastSequenceNumber}");

            state.DoNormalShutdown(10);

            state.WaitRun();

            Assert.True(state.Processor.TotalErrors == 0, $"Errors found {state.Processor.TotalErrors}");
            Assert.Null(state.ShutdownException);
        }

        [Fact]
        [DisplayTestMethodName]
        void UseInitialPositionProviderTest()
        {
            TestState state = new TestState();
            state.Initialize("UseInitialPositionProvider", 1, 0);
            const long firstSequenceNumber = 3456L;
            state.Options.InitialPositionProvider = (partitionId) => { return EventPosition.FromSequenceNumber(firstSequenceNumber); };

            Microsoft.Azure.EventHubs.ServiceFabricProcessor.ServiceFabricProcessor sfp =
                new Microsoft.Azure.EventHubs.ServiceFabricProcessor.ServiceFabricProcessor(
                    state.ServiceUri,
                    state.ServicePartitionId,
                    state.StateManager,
                    state.StatefulServicePartition,
                    state.Processor,
                    state.ConnectionString,
                    "$Default",
                    state.Options);
            sfp.MockMode = state.PartitionLister;
            sfp.EventHubClientFactory = new EventHubMocks.EventHubClientFactoryMock(1);

            state.PrepareToRun();
            state.StartRun(sfp);

            state.RunForNBatches(20, 10);

            state.WaitRun();

            Assert.True(state.Processor.FirstEvent.SystemProperties.SequenceNumber == (firstSequenceNumber + 1L),
                $"Got unexpected first sequence number {state.Processor.FirstEvent.SystemProperties.SequenceNumber}");
            Assert.True(state.Processor.TotalErrors == 0, $"Errors found {state.Processor.TotalErrors}");
            Assert.Null(state.ShutdownException);
        }

        [Fact]
        [DisplayTestMethodName]
        void IgnoreInitialPositionProviderTest()
        {
            TestState state = new TestState();
            state.Initialize("IgnoreInitialPositionProvider", 1, 0);
            const long ippSequenceNumber = 3456L;
            state.Options.InitialPositionProvider = (partitionId) => { return EventPosition.FromSequenceNumber(ippSequenceNumber); };

            // Fake up a checkpoint using code borrowed from ReliableDictionaryCheckpointManager
            IReliableDictionary<string, Dictionary<string, object>> store =
                state.StateManager.GetOrAddAsync<IReliableDictionary<string, Dictionary<string, object>>>("EventProcessorCheckpointDictionary").Result;
            const long checkpointSequenceNumber = 8888L;
            Checkpoint fake = new Checkpoint((checkpointSequenceNumber * 100L).ToString(), checkpointSequenceNumber);
            using (ITransaction tx = state.StateManager.CreateTransaction())
            {
                store.SetAsync(tx, "0", fake.ToDictionary(), TimeSpan.FromSeconds(5.0), CancellationToken.None).Wait();
                tx.CommitAsync().Wait();
            }

            Microsoft.Azure.EventHubs.ServiceFabricProcessor.ServiceFabricProcessor sfp =
                new Microsoft.Azure.EventHubs.ServiceFabricProcessor.ServiceFabricProcessor(
                    state.ServiceUri,
                    state.ServicePartitionId,
                    state.StateManager,
                    state.StatefulServicePartition,
                    state.Processor,
                    state.ConnectionString,
                    "$Default",
                    state.Options);
            sfp.MockMode = state.PartitionLister;
            sfp.EventHubClientFactory = new EventHubMocks.EventHubClientFactoryMock(1);

            state.PrepareToRun();
            state.StartRun(sfp);

            state.RunForNBatches(20, 10);

            state.WaitRun();

            Assert.True(state.Processor.FirstEvent.SystemProperties.SequenceNumber == (checkpointSequenceNumber + 1L),
                $"Got unexpected first sequence number {state.Processor.FirstEvent.SystemProperties.SequenceNumber}");
            Assert.True(state.Processor.TotalErrors == 0, $"Errors found {state.Processor.TotalErrors}");
            Assert.Null(state.ShutdownException);
        }

        [Fact]
        [DisplayTestMethodName]
        void RuntimeInformationTest()
        {
            TestState state = new TestState();
            state.Initialize("RuntimeInformation", 1, 0);
            state.Options.EnableReceiverRuntimeMetric = true;

            Microsoft.Azure.EventHubs.ServiceFabricProcessor.ServiceFabricProcessor sfp =
                new Microsoft.Azure.EventHubs.ServiceFabricProcessor.ServiceFabricProcessor(
                    state.ServiceUri,
                    state.ServicePartitionId,
                    state.StateManager,
                    state.StatefulServicePartition,
                    state.Processor,
                    state.ConnectionString,
                    "$Default",
                    state.Options);
            sfp.MockMode = state.PartitionLister;
            sfp.EventHubClientFactory = new EventHubMocks.EventHubClientFactoryMock(1);

            state.PrepareToRun();
            state.StartRun(sfp);

            state.RunForNBatches(20, 10);

            state.WaitRun();

            Assert.True(state.Processor.LatestContext.RuntimeInformation.LastSequenceNumber == state.Processor.LastEvent.SystemProperties.SequenceNumber);

            Assert.True(state.Processor.TotalErrors == 0, $"Errors found {state.Processor.TotalErrors}");
            Assert.Null(state.ShutdownException);
        }
    }
}
