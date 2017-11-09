// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;

    /// <summary>
    /// Diagnostic Source util for "Microsoft.Azure.EventHubs" source.
    /// </summary>
    internal static class EventHubsDiagnosticSource
    {
        public const string DiagnosticSourceName = "Microsoft.Azure.EventHubs";

        public const string SendActivityName = DiagnosticSourceName + ".Send";
        public const string SendActivityStartName = SendActivityName + ".Start";
        public const string SendActivityExceptionName = SendActivityName + ".Exception";

        public const string ReceiveActivityName = DiagnosticSourceName + ".Receive";
        public const string ReceiveActivityStartName = ReceiveActivityName + ".Start";
        public const string ReceiveActivityExceptionName = ReceiveActivityName + ".Exception";

        internal static readonly DiagnosticListener DiagnosticListener = new DiagnosticListener(DiagnosticSourceName);

        internal static Activity StartSendActivity(string clientId, EventHubsConnectionStringBuilder csb, string partitionKey, IEnumerable<EventData> eventDatas, int count)
        {
            // skip if diagnostic source not enabled
            if (!DiagnosticListener.IsEnabled())
            {
                return null;
            }

            // skip if no listeners for this "Send" activity 
            if (!DiagnosticListener.IsEnabled(SendActivityName, csb.Endpoint, csb.EntityPath))
            {
                return null;
            }

            Activity activity = new Activity(SendActivityName);

            activity.AddTag("peer.hostname", csb.Endpoint.Host);
            activity.AddTag("eh.event_hub_name", csb.EntityPath);
            activity.AddTag("eh.partition_key", partitionKey);
            activity.AddTag("eh.event_count", count.ToString());
            activity.AddTag("eh.client_id", clientId);

            // in many cases activity start event is not interesting, 
            // in that case start activity without firing event
            if (DiagnosticListener.IsEnabled(SendActivityStartName))
            {
                DiagnosticListener.StartActivity(activity,
                    new
                    {
                        Endpoint = csb.Endpoint,
                        EntityPath = csb.EntityPath,
                        PartitionKey = partitionKey,
                        EventDatas = eventDatas
                    });
            }
            else
            {
                activity.Start();
            }

            return activity;
        }

        internal static void FailSendActivity(Activity activity, EventHubsConnectionStringBuilder csb, string partitionKey, IEnumerable<EventData> eventDatas, Exception ex)
        {
            if (!DiagnosticListener.IsEnabled() || !DiagnosticListener.IsEnabled(SendActivityExceptionName))
            {
                return;
            }

            DiagnosticListener.Write(SendActivityExceptionName,
                new
                {
                    Endpoint = csb.Endpoint,
                    EntityPath = csb.EntityPath,
                    PartitionKey = partitionKey,
                    EventDatas = eventDatas,
                    Exception = ex
                });
        }

        internal static void StopSendActivity(Activity activity, EventHubsConnectionStringBuilder csb, string partitionKey, IEnumerable<EventData> eventDatas, Task sendTask)
        {
            if (activity == null)
            {
                return;
            }

            DiagnosticListener.StopActivity(activity,
                new
                {
                    Endpoint = csb.Endpoint,
                    EntityPath = csb.EntityPath,
                    PartitionKey = partitionKey,
                    EventDatas = eventDatas,
                    TaskStatus = sendTask?.Status
                });
        }

        internal static Activity StartReceiveActivity(string clientId, EventHubsConnectionStringBuilder csb, string partitionKey, string consumerGroup, string startOffset)
        {
            // skip if diagnostic source not enabled
            if (!DiagnosticListener.IsEnabled())
            {
                return null;
            }

            // skip if no listeners for this "Receive" activity 
            if (!DiagnosticListener.IsEnabled(ReceiveActivityName, csb.Endpoint, csb.EntityPath))
            {
                return null;
            }

            Activity activity = new Activity(ReceiveActivityName);

            // extract activity tags from input
            activity.AddTag("peer.hostname", csb.Endpoint.Host);
            activity.AddTag("eh.event_hub_name", csb.EntityPath);
            activity.AddTag("eh.partition_key", partitionKey);
            activity.AddTag("eh.consumer_group", consumerGroup);
            activity.AddTag("eh.start_offset", startOffset);
            activity.AddTag("eh.client_id", clientId);

            // in many cases activity start event is not interesting, 
            // in that case start activity without firing event
            if (DiagnosticListener.IsEnabled(ReceiveActivityStartName))
            {
                DiagnosticListener.StartActivity(activity,
                    new
                    {
                        Endpoint = csb.Endpoint,
                        EntityPath = csb.EntityPath,
                        PartitionKey = partitionKey,
                        ConsumerGroup = consumerGroup
                    });
            }
            else
            {
                activity.Start();
            }

            return activity;
        }

        internal static void FailReceiveActivity(Activity activity, EventHubsConnectionStringBuilder csb, string partitionKey, string consumerGroup, Exception ex)
        {
            // TODO consider enriching activity with data from exception

            if (!DiagnosticListener.IsEnabled() || !DiagnosticListener.IsEnabled(ReceiveActivityExceptionName))
            {
                return;
            }

            DiagnosticListener.Write(ReceiveActivityExceptionName,
                new
                {
                    Endpoint = csb.Endpoint,
                    EntityPath = csb.EntityPath,
                    PartitionKey = partitionKey,
                    ConsumerGroup = consumerGroup,
                    Exception = ex
                });
        }

        internal static void StopReceiveActivity(Activity activity, EventHubsConnectionStringBuilder csb, string partitionKey, string consumerGroup, IList<EventData> events, Task receiveTask)
        {
            if (activity == null)
            {
                return;
            }

            activity.AddTag("eh.event_count", (events?.Count ?? 0).ToString());

            DiagnosticListener.StopActivity(activity,
                new
                {
                    Endpoint = csb.Endpoint,
                    EntityPath = csb.EntityPath,
                    PartitionKey = partitionKey,
                    ConsumerGroup = consumerGroup,
                    EventDatas = events,
                    TaskStatus = receiveTask?.Status
                });
        }
    }
}
