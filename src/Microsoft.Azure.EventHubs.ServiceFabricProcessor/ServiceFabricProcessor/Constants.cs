// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    class Constants
    {
        internal static readonly string ConfigurationPackageName = "Config";

        internal static readonly int RetryCount = 5;

        internal static readonly TimeSpan MetricReportingInterval = TimeSpan.FromMinutes(1.0);
        internal static readonly string UserLoadMetricName = "UserMetric";

        internal static readonly TimeSpan UpdateServiceDescriptionTimeout = TimeSpan.FromSeconds(30.0);

        internal static readonly string EventProcessorConfigSectionName = "EventProcessorConfig";
        internal static readonly string EventHubConnectionStringConfigName = "EventHubConnectionString";
        internal static readonly string EventHubConsumerGroupConfigName = "EventHubConsumerGroup";
        internal static readonly string EventHubConsumerGroupConfigDefault = PartitionReceiver.DefaultConsumerGroupName;

        internal static readonly TimeSpan ReliableDictionaryTimeout = TimeSpan.FromSeconds(10.0); // arbitrary
        internal static readonly string CheckpointDictionaryName = "EventProcessorCheckpointDictionary";
        internal static readonly string CheckpointPropertyVersion = "version";
        internal static readonly string CheckpointPropertyValid = "valid";
        internal static readonly string CheckpointPropertyOffsetV1 = "offsetV1";
        internal static readonly string CheckpointPropertySequenceNumberV1 = "sequenceNumberV1";
        internal static readonly string EpochDictionaryName = "EventProcessorEpochDictionary";
    }
}
