// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    public class EventProcessorOptions
    {
        public EventProcessorOptions()
        {
            this.MaxBatchSize = 10;
            this.PrefetchCount = 300;
            this.ReceiveTimeout = TimeSpan.FromMinutes(1);
            this.InitialOffsetProvider = partitionId => EventPosition.FromStart();
        }

        public int MaxBatchSize { get; set; }

        public int PrefetchCount { get; set; }

        public TimeSpan ReceiveTimeout { get; set; }

        public bool EnableReceiverRuntimeMetric { get; set; }

        public bool InvokeProcessorAfterReceiveTimeout { get; set; }

        public Func<string, EventPosition> InitialOffsetProvider { get; set; }
    }
}
