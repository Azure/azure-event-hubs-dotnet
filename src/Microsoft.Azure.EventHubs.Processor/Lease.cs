// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Processor
{
    using System.Threading.Tasks;

    public class Lease
    {
        protected Lease()
        {
        }

        protected Lease(string partitionId)
        {
            this.PartitionId = partitionId;
            this.Owner = string.Empty;
            this.Token = string.Empty;
        }

        protected Lease(Lease source)
        {
            this.PartitionId = source.PartitionId;
            this.Epoch = source.Epoch;
            this.Owner = source.Owner;
            this.Token = source.Token;
        }

        public string Offset { get; set; }

        public long SequenceNumber { get; set; }

        public string PartitionId { get; set; }

        public string Owner { get; set; }

        public string Token { get; set; }

        public long Epoch { get; set; }

        public virtual Task<bool> IsExpired() 
        {
            // By default lease never expires.
            // Deriving class will implement the lease expiry logic.
            return Task.FromResult(false);
        }

        internal long IncrementEpoch()
        {
            this.Epoch++;
            return this.Epoch;
        }
    }
}