// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;

    public class EventHubRuntimeInformation
    {
        public string Path { get; set; }

        internal string Type { get; set; }

        public DateTime CreatedAt { get; set; }

        public int PartitionCount { get; set; }

        public string[] PartitionIds { get; set; }
    }
}