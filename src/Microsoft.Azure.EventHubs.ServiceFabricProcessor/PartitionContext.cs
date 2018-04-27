// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    public class PartitionContext
    {
        readonly private ICheckpointMananger checkpointMananger;

        public PartitionContext(CancellationToken cancellationToken, string partitionId, string eventHubPath, string consumerGroupName, ICheckpointMananger checkpointMananger)
        {
            this.CancellationToken = cancellationToken;
            this.PartitionId = partitionId;
            this.EventHubPath = eventHubPath;
            this.ConsumerGroupName = consumerGroupName;

            this.checkpointMananger = checkpointMananger;
        }

        public CancellationToken CancellationToken { get; private set; }

        public string ConsumerGroupName { get; private set; }

        public string EventHubPath { get; private set; }

        public string PartitionId { get; private set; }

        // FOO receiverRuntimeInformation

        internal string Offset { get; set; }

        internal long SequenceNumber { get; set; }

        internal void SetOffsetAndSequenceNumber(EventHubWrappers.IEventData eventData)
        {
            this.Offset = eventData.SystemProperties.Offset;
            this.SequenceNumber = eventData.SystemProperties.SequenceNumber;
        }

        public async Task CheckpointAsync()
        {
            await CheckpointAsync(new Checkpoint(this.Offset, this.SequenceNumber));
        }

        public async Task CheckpointAsync(EventHubWrappers.IEventData eventData)
        {
            await CheckpointAsync(new Checkpoint(eventData.SystemProperties.Offset, eventData.SystemProperties.SequenceNumber));
        }

        private async Task CheckpointAsync(Checkpoint checkpoint)
        {
            await this.checkpointMananger.UpdateCheckpointAsync(this.PartitionId, checkpoint, this.CancellationToken);
        }
    }
}
