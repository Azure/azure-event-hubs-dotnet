// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    class DefaultEventProcessorFactory<TEventProcessor> : IEventProcessorFactory
        where TEventProcessor : IEventProcessor, new()
    {
        public DefaultEventProcessorFactory()
        {
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context)
        {
            return new TEventProcessor();
        }
    }
}
