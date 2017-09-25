// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    /// <summary>
    /// Options define partiton-key and max-message size while creating a batch. <see cref="EventDataBatch"/> 
    /// </summary>
    public class BatchOptions
    {
        /// <summary>
        /// 
        /// </summary>
        public string PartitionKey { get; set; }

        public long MaxMessageSize { get; set }
    }
}
