// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Processor
{
    using System;

    /// <summary>
    /// Options to control various aspects of partition distribution happening within <see cref="EventProcessorHost"/> instance.
    /// </summary> 
    public class PartitionManagerOptions
    {
        static readonly TimeSpan DefaultRenewInterval = TimeSpan.FromSeconds(10);
        static readonly TimeSpan DefaultLeaseDuration = TimeSpan.FromSeconds(30);

        /// <summary>Initializes a new instance of the <see cref="PartitionManagerOptions" /> class.</summary>
        public PartitionManagerOptions()
        {
            RenewInterval = DefaultRenewInterval;
            LeaseDuration = DefaultLeaseDuration;
        }

        /// <summary>
        /// Renew interval for all leases for partitions currently held by <see cref="EventProcessorHost"/> instance.
        /// </summary>
        public TimeSpan RenewInterval { get; set; }

        /// <summary>
        /// Interval for which the lease is taken on Azure Blob representing an EventHub partition.  If the lease is not renewed within this 
        /// interval, it will cause it to expire and ownership of the partition will move to another <see cref="EventProcessorHost"/> instance.
        /// </summary>
        public TimeSpan LeaseDuration { get; set; }

        /// <summary>
        /// Creates an instance of <see cref="PartitionManagerOptions"/> with following default values:
        ///     a) RenewInterval = 10 seconds
        ///     c) DefaultLeaseInterval = 30 seconds
        /// </summary>
        public static PartitionManagerOptions DefaultOptions
        {
            get
            {
                return new PartitionManagerOptions();
            }
        }
    }
}
