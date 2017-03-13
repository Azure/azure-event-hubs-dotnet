// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Processor
{
    using System;
    using System.Threading.Tasks;

    public class PartitionContext
    {
        readonly EventProcessorHost host;

        internal PartitionContext(EventProcessorHost host, string partitionId, string eventHubPath, string consumerGroupName)
        {
            this.host = host;
            this.PartitionId = partitionId;
            this.EventHubPath = eventHubPath;
            this.ConsumerGroupName = consumerGroupName;
            this.ThisLock = new object();
            this.Offset = PartitionReceiver.StartOfStream;
            this.SequenceNumber = 0;
        }

        public string ConsumerGroupName { get; }

        public string EventHubPath { get; }

        public string PartitionId { get; }

        public string Owner
        {
            get
            {
                return this.Lease.Owner;
            }
        }

        internal string Offset { get; set; }

        internal long SequenceNumber { get; set; }

        // Unlike other properties which are immutable after creation, the lease is updated dynamically and needs a setter.
        internal Lease Lease { get; set; }

        object ThisLock { get; }

        internal void SetOffsetAndSequenceNumber(EventData eventData)
        {
            if (eventData == null)
            {
                throw new ArgumentNullException(nameof(eventData));
            }

            lock (this.ThisLock)
            {
                this.Offset = eventData.SystemProperties.Offset;
                this.SequenceNumber = eventData.SystemProperties.SequenceNumber;
            }
        }

        internal async Task<object> GetInitialOffsetAsync() // throws InterruptedException, ExecutionException
        {
            Checkpoint startingCheckpoint = await this.host.CheckpointManager.GetCheckpointAsync(this.PartitionId).ConfigureAwait(false);
            object startAt;

            if (startingCheckpoint == null)
            {
                // No checkpoint was ever stored. Use the initialOffsetProvider instead.
                Func<string, object> initialOffsetProvider = this.host.EventProcessorOptions.InitialOffsetProvider;
                ProcessorEventSource.Log.PartitionPumpInfo(this.host.Id, this.PartitionId, "Calling user-provided initial offset provider");
                startAt = initialOffsetProvider(this.PartitionId);

                if (startAt is string)
                {
                    this.Offset = (string)startAt;
                    this.SequenceNumber = 0; // TODO we use sequenceNumber to check for regression of offset, 0 could be a problem until it gets updated from an event
                    ProcessorEventSource.Log.PartitionPumpInfo(this.host.Id, this.PartitionId, $"Initial offset/sequenceNumber provided: {this.Offset}/{this.SequenceNumber}");
                }
                else if (startAt is DateTime)
                {
                    // can't set offset/sequenceNumber
                    ProcessorEventSource.Log.PartitionPumpInfo(this.host.Id, this.PartitionId, $"Initial timestamp provided: {(DateTime)startAt}");
                }
                else
                {
                    throw new ArgumentException("Unexpected object type returned by user-provided initialOffsetProvider");
                }
    	    }
    	    else
    	    {
                this.Offset = startingCheckpoint.Offset;
	    	    this.SequenceNumber = startingCheckpoint.SequenceNumber;
                ProcessorEventSource.Log.PartitionPumpInfo(this.host.Id, this.PartitionId, $"Retrieved starting offset/sequenceNumber: {this.Offset}/{this.SequenceNumber}");
                startAt = this.Offset;
            }

    	    return startAt;
        }

        /// <summary>
        /// Writes the current offset and sequenceNumber to the checkpoint store via the checkpoint manager.
        /// </summary>
        public Task CheckpointAsync()
        {
    	    // Capture the current offset and sequenceNumber. Synchronize to be sure we get a matched pair
    	    // instead of catching an update halfway through. Do the capturing here because by the time the checkpoint
    	    // task runs, the fields in this object may have changed, but we should only write to store what the user
    	    // has directed us to write.
    	    Checkpoint capturedCheckpoint;
            lock(this.ThisLock)
            {
                capturedCheckpoint = new Checkpoint(this.PartitionId, this.Offset, this.SequenceNumber);
            }

            return this.PersistCheckpointAsync(capturedCheckpoint);
        }

        /// <summary>
        /// Stores the offset and sequenceNumber from the provided received EventData instance, then writes those
        /// values to the checkpoint store via the checkpoint manager.
        /// </summary>
        /// <param name="eventData">A received EventData with valid offset and sequenceNumber</param>
        /// <exception cref="ArgumentNullException">If suplied eventData is null</exception>
        /// <exception cref="ArgumentOutOfRangeException">If the sequenceNumber is less than the last checkpointed value</exception>
        public Task CheckpointAsync(EventData eventData)
        {
            if (eventData == null)
            {
                throw new ArgumentNullException("eventData");
            }
            
            // We have never seen this sequence number yet
            if (eventData.SystemProperties.SequenceNumber > this.SequenceNumber)
            {
                throw new ArgumentOutOfRangeException("eventData.SystemProperties.SequenceNumber");
            }

            return this.PersistCheckpointAsync(new Checkpoint(this.PartitionId, eventData.SystemProperties.Offset, eventData.SystemProperties.SequenceNumber));
        }

        public override string ToString()
        {
            return $"PartitionContext({this.EventHubPath}/{this.ConsumerGroupName}/{this.PartitionId}/{this.SequenceNumber})";
        }

        async Task PersistCheckpointAsync(Checkpoint checkpoint)
        {
            ProcessorEventSource.Log.PartitionPumpCheckpointStart(this.host.Id, checkpoint.PartitionId, checkpoint.Offset, checkpoint.SequenceNumber);
            try
            {
                Checkpoint inStoreCheckpoint = await this.host.CheckpointManager.GetCheckpointAsync(checkpoint.PartitionId).ConfigureAwait(false);
                if (inStoreCheckpoint == null || checkpoint.SequenceNumber >= inStoreCheckpoint.SequenceNumber)
                {
                    if (inStoreCheckpoint == null)
                    {
                        await this.host.CheckpointManager.CreateCheckpointIfNotExistsAsync(checkpoint.PartitionId).ConfigureAwait(false);
                    }

                    await this.host.CheckpointManager.UpdateCheckpointAsync(this.Lease, checkpoint).ContinueWith((obj) =>
                    {
                        this.Lease.Offset = checkpoint.Offset;
                        this.Lease.SequenceNumber = checkpoint.SequenceNumber;
                    }, TaskContinuationOptions.OnlyOnRanToCompletion).ConfigureAwait(false);
                }
                else
                {
                    string msg = $"Ignoring out of date checkpoint with offset {checkpoint.Offset}/sequence number {checkpoint.SequenceNumber}" +
                            $" because current persisted checkpoint has higher offset {inStoreCheckpoint.Offset}/sequence number {inStoreCheckpoint.SequenceNumber}";
                    ProcessorEventSource.Log.PartitionPumpError(this.host.Id, checkpoint.PartitionId, msg);
                    throw new ArgumentOutOfRangeException("offset/sequenceNumber", msg);
                }
            }
            catch (Exception e)
            {
                ProcessorEventSource.Log.PartitionPumpCheckpointError(this.host.Id, checkpoint.PartitionId, e.ToString());
                throw;
            }
            finally
            {
                ProcessorEventSource.Log.PartitionPumpCheckpointStop(this.host.Id, checkpoint.PartitionId);
            }
        }
    }
}