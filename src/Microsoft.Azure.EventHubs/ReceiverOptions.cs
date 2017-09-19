// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    /// <summary>
    /// Represents options can be set during the creation of a event hub receiver.
    /// </summary> 
    public class ReceiverOptions
    {
        /// <summary> Gets or sets a value indicating whether the runtime metric of a receiver is enabled. </summary>
        /// <value> true if a client wants to access <see cref="ReceiverRuntimeInformation"/> using <see cref="PartitionReceiver"/>. </value>
        public bool EnableReceiverRuntimeMetric
        {
            get; set;
        }
    }
}
