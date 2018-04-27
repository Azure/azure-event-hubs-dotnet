// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    public interface ICheckpointMananger
    {
        Task<bool> CheckpointStoreExistsAsync(CancellationToken cancellationToken);

        Task<bool> CreateCheckpointStoreIfNotExistsAsync(CancellationToken cancellationToken);

        Task<Checkpoint> CreateCheckpointIfNotExistsAsync(string partitionId, CancellationToken cancellationToken);

        // Returns null if checkpoint doesn't exist.
        Task<Checkpoint> GetCheckpointAsync(string partitionId, CancellationToken cancellationToken);

        Task UpdateCheckpointAsync(string partitionId, Checkpoint checkpoint, CancellationToken cancellationToken);

        //Task DeleteCheckpointAsync(string partitionId, CancellationToken cancellationToken);
    }
}
