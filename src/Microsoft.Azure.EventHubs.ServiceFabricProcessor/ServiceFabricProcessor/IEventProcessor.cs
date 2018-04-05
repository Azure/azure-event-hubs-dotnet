// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    public abstract class IEventProcessor
    {
        abstract public Task OpenAsync(CancellationToken cancellationToken, PartitionContext context);

        abstract public Task CloseAsync(PartitionContext context, CloseReason reason);

        abstract public Task ProcessEventsAsync(CancellationToken cancellationToken, PartitionContext context, IEnumerable<EventHubWrappers.IEventData> events);

        abstract public Task ProcessErrorAsync(PartitionContext context, Exception error);

        virtual public int GetLoadMetric(CancellationToken cancellationToken, PartitionContext context)
        {
            // By default all partitions have a metric of 1, so Service Fabric will balance primaries
            // across nodes simply by the number of primaries on a node. This can be overridden to return
            // more sophisticated metrics like number of events processed or CPU usage.
            return 1;
        }
    }
}
