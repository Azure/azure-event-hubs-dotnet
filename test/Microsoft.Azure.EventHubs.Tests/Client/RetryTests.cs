// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Tests.Client
{
    using System;
    using Xunit;

    public class RetryTests
    {
        [Fact]
        [DisplayTestMethodName]
        void ValidateRetryPolicyBuiltIn()
        {
            String clientId = "someClientEntity";
            RetryPolicy retry = RetryPolicy.Default;

            retry.IncrementRetryCount(clientId);
            TimeSpan? firstRetryInterval = retry.GetNextRetryInterval(clientId, new ServerBusyException(string.Empty), TimeSpan.FromSeconds(60));
            TestUtility.Log("firstRetryInterval: " + firstRetryInterval);
            Assert.True(firstRetryInterval != null);

            retry.IncrementRetryCount(clientId);
            TimeSpan? secondRetryInterval = retry.GetNextRetryInterval(clientId, new ServerBusyException(string.Empty), TimeSpan.FromSeconds(60));
            TestUtility.Log("secondRetryInterval: " + secondRetryInterval);

            Assert.True(secondRetryInterval != null);
            Assert.True(secondRetryInterval?.TotalMilliseconds > firstRetryInterval?.TotalMilliseconds);

            retry.IncrementRetryCount(clientId);
            TimeSpan? thirdRetryInterval = retry.GetNextRetryInterval(clientId, new ServerBusyException(string.Empty), TimeSpan.FromSeconds(60));
            TestUtility.Log("thirdRetryInterval: " + thirdRetryInterval);

            Assert.True(thirdRetryInterval != null);
            Assert.True(thirdRetryInterval?.TotalMilliseconds > secondRetryInterval?.TotalMilliseconds);

            retry.IncrementRetryCount(clientId);
            TimeSpan? fourthRetryInterval = retry.GetNextRetryInterval(clientId, new ServerBusyException(string.Empty), TimeSpan.FromSeconds(60));
            TestUtility.Log("fourthRetryInterval: " + fourthRetryInterval);

            Assert.True(fourthRetryInterval != null);
            Assert.True(fourthRetryInterval?.TotalMilliseconds > thirdRetryInterval?.TotalMilliseconds);

            retry.IncrementRetryCount(clientId);
            TimeSpan? fifthRetryInterval = retry.GetNextRetryInterval(clientId, new ServerBusyException(string.Empty), TimeSpan.FromSeconds(60));
            TestUtility.Log("fifthRetryInterval: " + fifthRetryInterval);

            Assert.True(fifthRetryInterval != null);
            Assert.True(fifthRetryInterval?.TotalMilliseconds > fourthRetryInterval?.TotalMilliseconds);

            retry.IncrementRetryCount(clientId);
            TimeSpan? sixthRetryInterval = retry.GetNextRetryInterval(clientId, new ServerBusyException(string.Empty), TimeSpan.FromSeconds(60));
            TestUtility.Log("sixthRetryInterval: " + sixthRetryInterval);

            Assert.True(sixthRetryInterval != null);
            Assert.True(sixthRetryInterval?.TotalMilliseconds > fifthRetryInterval?.TotalMilliseconds);

            retry.IncrementRetryCount(clientId);
            TimeSpan? seventhRetryInterval = retry.GetNextRetryInterval(clientId, new ServerBusyException(string.Empty), TimeSpan.FromSeconds(60));
            TestUtility.Log("seventhRetryInterval: " + seventhRetryInterval);

            Assert.True(seventhRetryInterval != null);
            Assert.True(seventhRetryInterval?.TotalMilliseconds > sixthRetryInterval?.TotalMilliseconds);

            retry.IncrementRetryCount(clientId);
            TimeSpan? nextRetryInterval = retry.GetNextRetryInterval(clientId, new EventHubsException(false), TimeSpan.FromSeconds(60));
            Assert.True(nextRetryInterval == null);

            retry.ResetRetryCount(clientId);
            retry.IncrementRetryCount(clientId);
            TimeSpan? firstRetryIntervalAfterReset = retry.GetNextRetryInterval(clientId, new ServerBusyException(string.Empty), TimeSpan.FromSeconds(60));
            Assert.True(firstRetryInterval.Equals(firstRetryIntervalAfterReset));

            retry = RetryPolicy.NoRetry;
            retry.IncrementRetryCount(clientId);
            TimeSpan? noRetryInterval = retry.GetNextRetryInterval(clientId, new ServerBusyException(string.Empty), TimeSpan.FromSeconds(60));
            Assert.True(noRetryInterval == null);
        }

        [Fact]
        [DisplayTestMethodName]
        void ValidateRetryPolicyCustom()
        {
            String clientId = "someClientEntity";

            // Retry up to 5 times.
            RetryPolicy retry = new RetryPolicyCustom(5);

            // Retry 4 times. These should allow retry.
            for (int i = 0; i < 4; i++)
            {
                retry.IncrementRetryCount(clientId);
                TimeSpan? thisRetryInterval = retry.GetNextRetryInterval(clientId, new ServerBusyException(string.Empty), TimeSpan.FromSeconds(60));
                TestUtility.Log("RetryInterval: " + thisRetryInterval);
                Assert.True(thisRetryInterval.Value.TotalSeconds == 2 + i);
            }

            // Retry 5th times. This should not allow retry.
            retry.IncrementRetryCount(clientId);
            TimeSpan? newRetryInterval = retry.GetNextRetryInterval(clientId, new ServerBusyException(string.Empty), TimeSpan.FromSeconds(60));
            TestUtility.Log("RetryInterval: " + newRetryInterval);
            Assert.True(newRetryInterval == null);
        }

        [Fact]
        [DisplayTestMethodName]
        void ChildEntityShouldInheritRetryPolicyFromParent()
        {
            var testMaxRetryCount = 99;

            var ehClient = EventHubClient.CreateFromConnectionString(TestUtility.EventHubsConnectionString);
            ehClient.RetryPolicy = new RetryPolicyCustom(testMaxRetryCount);

            // Validate partition sender inherits.
            var sender = ehClient.CreateEventSender("0");
            Assert.True(sender.RetryPolicy is RetryPolicyCustom, "Sender failed to inherit parent client's RetryPolicy setting.");
            Assert.True((sender.RetryPolicy as RetryPolicyCustom).maximumRetryCount == testMaxRetryCount,
                $"Retry policy on the sender shows testMaxRetryCount as {(sender.RetryPolicy as RetryPolicyCustom).maximumRetryCount}");

            // Validate partition receiver inherits.
            var receiver = ehClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, "0", PartitionReceiver.StartOfStream);
            Assert.True(receiver.RetryPolicy is RetryPolicyCustom, "Receiver failed to inherit parent client's RetryPolicy setting.");
            Assert.True((receiver.RetryPolicy as RetryPolicyCustom).maximumRetryCount == testMaxRetryCount,
                $"Retry policy on the receiver shows testMaxRetryCount as {(sender.RetryPolicy as RetryPolicyCustom).maximumRetryCount}");
        }

        public sealed class RetryPolicyCustom : RetryPolicy
        {
            public readonly int maximumRetryCount;

            public RetryPolicyCustom(int maximumRetryCount)
            {
                this.maximumRetryCount = maximumRetryCount;
            }

            protected override TimeSpan? OnGetNextRetryInterval(string clientId, Exception lastException, TimeSpan remainingTime, int baseWaitTimeSecs)
            {
                int currentRetryCount = this.GetRetryCount(clientId);

                if (currentRetryCount >= this.maximumRetryCount)
                {
                    TestUtility.Log("Not retrying: currentRetryCount >= maximumRetryCount");
                    return null;
                }

                TestUtility.Log("Retrying: currentRetryCount < maximumRetryCount");

                // Retry after 1 second + retry count.
                TimeSpan retryAfter = TimeSpan.FromSeconds(1 + currentRetryCount);

                return retryAfter;
            }

            public override RetryPolicy Clone()
            {
                return new RetryPolicyCustom(this.maximumRetryCount);
            }
        }
    }
}
