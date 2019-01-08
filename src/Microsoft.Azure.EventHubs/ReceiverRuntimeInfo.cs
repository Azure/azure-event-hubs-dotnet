// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;

    /// <summary>Represents the approximate receiver runtime information for a logical partition of an Event Hub.</summary>
    public class ReceiverRuntimeInformation
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="partitionId"></param>
        public ReceiverRuntimeInformation(string partitionId)
        {
            this.PartitionId = partitionId;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="e"></param>
        public void Update(EventData e)
        {
            this.LastSequenceNumber = e.LastSequenceNumber;
            this.LastEnqueuedTimeUtc = e.LastEnqueuedTime;
            this.LastEnqueuedOffset = e.LastEnqueuedOffset;
            this.RetrievalTime = e.RetrievalTime;
        }

        /// <summary>Gets the partition ID for a logical partition of an Event Hub.</summary>
        /// <value>The partition identifier.</value>
        public string PartitionId { get; internal set; }

        /// <summary>Gets the last sequence number of the event within the partition stream of the Event Hub.</summary>
        /// <value>The logical sequence number of the event.</value>
        public long LastSequenceNumber { get; internal set; }

        /// <summary>Gets the enqueued UTC time of the last event.</summary>
        /// <value>The enqueued time of the last event.</value>
        public DateTime LastEnqueuedTimeUtc { get; internal set; }

        /// <summary>Gets the offset of the last enqueued event.</summary>
        /// <value>The offset of the last enqueued event.</value>
        public string LastEnqueuedOffset { get; internal set; }

        /// <summary>Gets the time of when the runtime info was retrieved.</summary>
        /// <value>The enqueued time of the last event.</value>
        public DateTime RetrievalTime { get; internal set; }
    }
}
