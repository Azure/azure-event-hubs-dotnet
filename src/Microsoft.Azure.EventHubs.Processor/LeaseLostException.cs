// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Processor
{
    using System;

    public class LeaseLostException : Exception
    {
        readonly string partitionId;

        internal LeaseLostException(string partitionId, Exception innerException)
            : base(string.Empty, innerException)
        {
            if (partitionId == null)
            {
                throw new ArgumentNullException(nameof(partitionId));
            }

            this.partitionId = partitionId;
        }

        public string PartitionId
        {
            get { return this.partitionId; }
        }
    }
}