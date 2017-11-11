// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Tests.Client
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Xunit;

   public class DiagnosticsTests : ClientTestBase
    {
        protected ConcurrentQueue<(string eventName, object payload, Activity activity)> events;
        protected FakeDiagnosticListener listener;
        protected IDisposable subscription;
        protected const int maxWaitSec = 10;

        public DiagnosticsTests()
        {
            this.events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();
            this.listener = new FakeDiagnosticListener(kvp =>
            {
                TestUtility.Log($"Diagnostics event: {kvp.Key}, Activity Id: {Activity.Current?.Id}");
                if (kvp.Key.Contains("Exception"))
                {
                    TestUtility.Log($"Exception {kvp.Value}");
                }

                this.events.Enqueue((kvp.Key, kvp.Value, Activity.Current));
            });
            this.subscription = DiagnosticListener.AllListeners.Subscribe(this.listener);
        }

        #region Tests

        #region Send

        [Fact]
        [DisplayTestMethodName]
        async Task SendFiresEvents()
        {
            string partitionKey = "SomePartitionKeyHere";

            TestUtility.Log("Sending single Event via EventHubClient produces diagnostic events");
            Activity parentActivity = new Activity("test").AddBaggage("k1", "v1").AddBaggage("k2", "v2");

            // enable Send .Start & .Stop events
            this.listener.Enable((name, queueName, arg) => name.Contains("Send") && !name.EndsWith(".Exception"));

            parentActivity.Start();

            var eventData = new EventData(Encoding.UTF8.GetBytes("Hello EventHub by partitionKey!"));
            await this.EventHubClient.SendAsync(eventData, partitionKey);

            parentActivity.Stop();

            Assert.True(this.events.TryDequeue(out var sendStart));
            AssertSendStart(sendStart.eventName, sendStart.payload, sendStart.activity, parentActivity, partitionKey: partitionKey);

            Assert.True(this.events.TryDequeue(out var sendStop));
            AssertSendStop(sendStop.eventName, sendStop.payload, sendStop.activity, sendStart.activity, partitionKey: partitionKey);

            // no more events
            Assert.False(this.events.TryDequeue(out var evnt));
        }

        [Fact]
        [DisplayTestMethodName]
        async Task SendFiresExceptionEvents()
        {
            string partitionKey = "SomePartitionKeyHere";

            TestUtility.Log("Sending single Event via EventHubClient produces diagnostic events for exception");
            Activity parentActivity = new Activity("test").AddBaggage("k1", "v1").AddBaggage("k2", "v2");

            // enable Send .Exception & .Stop events
            this.listener.Enable((name, queueName, arg) => name.Contains("Send") && !name.EndsWith(".Start"));

            parentActivity.Start();

            // Events data size limit is 256kb - larger one will results in exception from within Send method
            var eventData = new EventData(new byte[300 * 1024 * 1024]);

            try
            {
                await this.EventHubClient.SendAsync(eventData, partitionKey);
                Assert.True(false, "Exception was expected but not thrown");
            }
            catch (Exception)
            { }

            parentActivity.Stop();
            
            Assert.True(this.events.TryDequeue(out var exception));
            AssertSendException(exception.eventName, exception.payload, exception.activity, null, partitionKey);

            Assert.True(this.events.TryDequeue(out var sendStop));
            AssertSendStop(sendStop.eventName, sendStop.payload, sendStop.activity, null, partitionKey, isFaulted: true);

            Assert.Equal(sendStop.activity, exception.activity);

            // no more events
            Assert.False(this.events.TryDequeue(out var evnt));
        }

        #endregion Send

        #region Partition Sender

        [Fact]
        [DisplayTestMethodName]
        async Task PartitionSenderSendFiresEvents()
        {
            string partitionKey = "1";
            TestUtility.Log("Sending single Event via PartitionSender produces diagnostic events");

            PartitionSender partitionSender1 = this.EventHubClient.CreatePartitionSender(partitionKey);
            Activity parentActivity = new Activity("test").AddBaggage("k1", "v1").AddBaggage("k2", "v2");

            // enable Send .Start & .Stop events
            this.listener.Enable((name, queueName, arg) => name.Contains("Send") && !name.EndsWith(".Exception"));

            parentActivity.Start();

            try
            {
                var eventData = new EventData(Encoding.UTF8.GetBytes("Hello again EventHub Partition 1!"));
                await partitionSender1.SendAsync(eventData);
            }
            finally
            {
                await partitionSender1.CloseAsync();
            }

            parentActivity.Stop();

            Assert.True(this.events.TryDequeue(out var sendStart));
            AssertSendStart(sendStart.eventName, sendStart.payload, sendStart.activity, parentActivity, partitionKey);

            Assert.True(this.events.TryDequeue(out var sendStop));
            AssertSendStop(sendStop.eventName, sendStop.payload, sendStop.activity, sendStart.activity, partitionKey);

            // no more events
            Assert.False(this.events.TryDequeue(out var evnt));
        }

        [Fact]
        [DisplayTestMethodName]
        async Task PartitionSenderSendFiresExceptionEvents()
        {
            string partitionKey = "1";
            TestUtility.Log("Sending single Event via PartitionSender produces diagnostic events for exception");

            PartitionSender partitionSender1 = this.EventHubClient.CreatePartitionSender(partitionKey);
            Activity parentActivity = new Activity("test").AddBaggage("k1", "v1").AddBaggage("k2", "v2");

            // enable Send .Exception & .Stop events
            this.listener.Enable((name, queueName, arg) => name.Contains("Send") && !name.EndsWith(".Start"));

            parentActivity.Start();

            try
            {
                // Events data size limit is 256kb - larger one will results in exception from within Send method
                var eventData = new EventData(new byte[300 * 1024 * 1024]);
                await partitionSender1.SendAsync(eventData);
                Assert.True(false, "Exception was expected but not thrown");
            }
            catch (Exception)
            { }
            finally
            {
                await partitionSender1.CloseAsync();
            }

            parentActivity.Stop();

            Assert.True(this.events.TryDequeue(out var exception));
            AssertSendException(exception.eventName, exception.payload, exception.activity, null, partitionKey);

            Assert.True(this.events.TryDequeue(out var sendStop));
            AssertSendStop(sendStop.eventName, sendStop.payload, sendStop.activity, null, partitionKey, isFaulted: true);

            Assert.Equal(sendStop.activity, exception.activity);

            // no more events
            Assert.False(this.events.TryDequeue(out var evnt));
        }

        #endregion Partition Sender

        #region Partition Receiver

        [Fact]
        [DisplayTestMethodName]
        async Task PartitionReceiverReceiveFiresEvents()
        {
            string partitionKey = "2";
            string payloadString = "Hello EventHub!";

            TestUtility.Log("Receiving Events via PartitionReceiver produces diagnostic events");

            // enable Send & Receive .Start & .Stop events
            this.listener.Enable((name, queueName, arg) => !name.EndsWith(".Exception"));

            // send to have some data to receive
            var sendEvent = new EventData(Encoding.UTF8.GetBytes(payloadString));
            var receivedEvent = await SendAndReceiveEvent(partitionKey, sendEvent);
            Assert.True(Encoding.UTF8.GetString(receivedEvent.Body.Array) == payloadString, "Received payload string isn't the same as sent payload string.");

            Assert.True(this.events.TryDequeue(out var sendStart));
            AssertSendStart(sendStart.eventName, sendStart.payload, sendStart.activity, null, partitionKey);

            Assert.True(this.events.TryDequeue(out var sendStop));
            AssertSendStop(sendStop.eventName, sendStop.payload, sendStop.activity, null, partitionKey);

            Assert.True(this.events.TryDequeue(out var receiveStart));
            AssertReceiveStart(receiveStart.eventName, receiveStart.payload, receiveStart.activity, partitionKey);

            Assert.True(this.events.TryDequeue(out var receiveStop));
            AssertReceiveStop(receiveStop.eventName, receiveStop.payload, receiveStop.activity, receiveStart.activity, partitionKey);

            // no more events
            Assert.False(this.events.TryDequeue(out var evnt));
        }

        #endregion Partition Receiver

        #endregion Tests

        #region Assertion Helpers

        #region Send

        protected void AssertSendStart(string name, object payload, Activity activity, Activity parentActivity, string partitionKey, int eventCount = 1)
        {
            Assert.Equal("Microsoft.Azure.EventHubs.Send.Start", name);
            AssertCommonPayloadProperties(payload, partitionKey);
            var eventDatas = GetPropertyValueFromAnonymousTypeInstance<IList<EventData>>(payload, "EventDatas");
            Assert.Equal(eventCount, eventDatas.Count);

            Assert.NotNull(activity);
            Assert.Equal(parentActivity, activity.Parent);

            AssertTagMatches(activity, "peer.hostname", this.EventHubClient.ConnectionStringBuilder.Endpoint.Host);
            AssertTagMatches(activity, "eh.event_hub_name", this.EventHubClient.ConnectionStringBuilder.EntityPath);
            if (partitionKey != null)
            {
                AssertTagMatches(activity, "eh.partition_key", partitionKey);
            }
            AssertTagMatches(activity, "eh.event_count", eventCount.ToString());
            AssertTagExists(activity, "eh.client_id");
        }

        protected void AssertSendException(string name, object payload, Activity activity, Activity parentActivity, string partitionKey)
        {
            Assert.Equal("Microsoft.Azure.EventHubs.Send.Exception", name);
            AssertCommonPayloadProperties(payload, partitionKey);

            GetPropertyValueFromAnonymousTypeInstance<Exception>(payload, "Exception");

            Assert.NotNull(activity);
            if (parentActivity != null)
            {
                Assert.Equal(parentActivity, activity.Parent);
            }

            var eventDatas = GetPropertyValueFromAnonymousTypeInstance<IList<EventData>>(payload, "EventDatas");
            Assert.NotNull(eventDatas);
        }

        protected void AssertSendStop(string name, object payload, Activity activity, Activity sendActivity, string partitionKey, bool isFaulted = false)
        {
            Assert.Equal("Microsoft.Azure.EventHubs.Send.Stop", name);
            AssertCommonStopPayloadProperties(payload, partitionKey, isFaulted);

            if (sendActivity != null)
            {
                Assert.Equal(sendActivity, activity);
            }

            var eventDatas = GetPropertyValueFromAnonymousTypeInstance<IList<EventData>>(payload, "EventDatas");
            Assert.NotNull(eventDatas);
        }

        #endregion

        #region Receive

        protected void AssertReceiveStart(string name, object payload, Activity activity, string partitionKey)
        {
            Assert.Equal("Microsoft.Azure.EventHubs.Receive.Start", name);
            AssertCommonPayloadProperties(payload, partitionKey);

            Assert.NotNull(activity);
            Assert.Null(activity.Parent);

            AssertTagMatches(activity, "peer.hostname", this.EventHubClient.ConnectionStringBuilder.Endpoint.Host);
            AssertTagMatches(activity, "eh.event_hub_name", this.EventHubClient.ConnectionStringBuilder.EntityPath);
            if (partitionKey != null)
            {
                AssertTagMatches(activity, "eh.partition_key", partitionKey);
            }
            AssertTagExists(activity, "eh.event_count");
            AssertTagExists(activity, "eh.client_id");

            AssertTagExists(activity, "eh.consumer_group");
            AssertTagExists(activity, "eh.start_offset");
        }

        protected void AssertReceiveStop(string name, object payload, Activity activity, Activity receiveActivity, string partitionKey, bool isFaulted = false)
        {
            Assert.Equal("Microsoft.Azure.EventHubs.Receive.Stop", name);
            AssertCommonStopPayloadProperties(payload, partitionKey, isFaulted);

            if (receiveActivity != null)
            {
                Assert.Equal(receiveActivity, activity);
            }
        }

        #endregion Receive

        #region Common

        protected void AssertTagExists(Activity activity, string tagName)
        {
            Assert.Contains(tagName, activity.Tags.Select(t => t.Key));
        }

        protected void AssertTagMatches(Activity activity, string tagName, string tagValue)
        {
            Assert.Contains(tagName, activity.Tags.Select(t => t.Key));
            Assert.Equal(tagValue, activity.Tags.Single(t => t.Key == tagName).Value);
        }

        protected void AssertCommonPayloadProperties(object eventPayload, string partitionKey)
        {
            var endpoint = GetPropertyValueFromAnonymousTypeInstance<Uri>(eventPayload, "Endpoint");
            Assert.Equal(this.EventHubClient.ConnectionStringBuilder.Endpoint, endpoint);

            var entityPath = GetPropertyValueFromAnonymousTypeInstance<string>(eventPayload, "EntityPath");
            Assert.Equal(this.EventHubClient.ConnectionStringBuilder.EntityPath, entityPath);

            var pKey = GetPropertyValueFromAnonymousTypeInstance<string>(eventPayload, "PartitionKey");
            Assert.Equal(partitionKey, pKey);
        }

        protected void AssertCommonStopPayloadProperties(object eventPayload, string partitionKey, bool isFaulted)
        {
            AssertCommonPayloadProperties(eventPayload, partitionKey);
            var status = GetPropertyValueFromAnonymousTypeInstance<TaskStatus>(eventPayload, "TaskStatus");
            Assert.Equal(isFaulted ? TaskStatus.Faulted : TaskStatus.RanToCompletion, status);
        }

        protected T GetPropertyValueFromAnonymousTypeInstance<T>(object obj, string propertyName)
        {
            Type t = obj.GetType();

            PropertyInfo p = t.GetRuntimeProperty(propertyName);

            object propertyValue = p.GetValue(obj);
            Assert.NotNull(propertyValue);
            Assert.IsAssignableFrom<T>(propertyValue);

            return (T)propertyValue;
        }

        #endregion Common

        #endregion Assertion Helpers
    }
}
