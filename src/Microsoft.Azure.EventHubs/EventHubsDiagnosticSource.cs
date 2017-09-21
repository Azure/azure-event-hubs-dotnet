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

        internal static bool IsEnabled => DiagnosticListener.IsEnabled();

        internal static Activity StartSendActivity(string clientId, EventHubsConnectionStringBuilder csb, string partitionKey, IEnumerable<EventData> eventDatas, int count)
        {
            // skip if diagnostic source not enabled
            if (!IsEnabled)
            {
                return null;
            }

            // skip if no listeners for this "Send" activity 
            if (!DiagnosticListener.IsEnabled(SendActivityName,
                new
                {
                    Endpoint = csb.Endpoint,
                    EntityPath = csb.EntityPath,
                    PartitionKey = partitionKey
                }))
            {
                return null;
            }

            Activity activity = new Activity(SendActivityName);

            // extract activity tags from input
            activity.AddTag("component", "Microsoft.Azure.EventHubs");
            activity.AddTag("span.kind", "producer");
            activity.AddTag("operation.name", $"Send");
            activity.AddTag("operation.data", $"{csb.EntityPath}/{partitionKey}");
            activity.AddTag("peer.service", "Azure Event Hub");
            activity.AddTag("peer.hostname", csb.Endpoint.OriginalString);
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
            // TODO consider enriching activity with data from exception

            if (!IsEnabled || !DiagnosticListener.IsEnabled(SendActivityExceptionName))
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

            // stop activity
            activity.AddTag("error", (sendTask?.Status == TaskStatus.RanToCompletion).ToString());
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
    }
}
