// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System.Threading;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    public interface IEventProcessorFactory
    {
        IEventProcessor CreateEventProcessor(CancellationToken cancellationToken, PartitionContext context);
    }
}
