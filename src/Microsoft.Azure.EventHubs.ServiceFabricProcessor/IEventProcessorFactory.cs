// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System.Threading;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    /// <summary>
    /// Interface for factories that dispense IEventProcessor instances.
    /// </summary>
    public interface IEventProcessorFactory
    {
        /// <summary>
        /// Dispense an IEventProcessor instance for the partition described by the PartitionContext.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        IEventProcessor CreateEventProcessor(CancellationToken cancellationToken, PartitionContext context);
    }
}
