﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using System.Collections.Concurrent;

    public abstract class RetryPolicy
    {
        const int DefaultRetryMaxCount = 10;

        static readonly TimeSpan DefaultRetryMinBackoff = TimeSpan.Zero;
        static readonly TimeSpan DefaultRetryMaxBackoff = TimeSpan.FromSeconds(30);

        // Same retry policy may be used by multiple senders and receivers.
        // Because of this we keep track of retry counters in a concurrent dictionary.
        ConcurrentDictionary<string, int> retryCounts;
        object serverBusySync;

        protected RetryPolicy()
        {
            this.retryCounts = new ConcurrentDictionary<string, int>();
            this.serverBusySync = new object();
        }

        public static RetryPolicy Default
        {
            get
            {
                return new RetryExponential(DefaultRetryMinBackoff, DefaultRetryMaxBackoff, DefaultRetryMaxCount);
            }
        }

        public static RetryPolicy NoRetry
        {
            get
            {
                return new RetryExponential(TimeSpan.Zero, TimeSpan.Zero, 0);
            }
        }

        public static bool IsRetryableException(Exception exception)
        {
            if (exception == null)
            {
                throw new ArgumentNullException("exception");
            }

            if (exception is EventHubsException)
            {
                return ((EventHubsException)exception).IsTransient;
            }
            else if (exception is OperationCanceledException)
            {
                return true;
            }

            return false;
        }

        public void IncrementRetryCount(string clientId)
        {
            int retryCount;
            this.retryCounts.TryGetValue(clientId, out retryCount);
            this.retryCounts[clientId] = retryCount + 1;
        }

        public void ResetRetryCount(string clientId)
        {
            int currentRetryCount;
            this.retryCounts.TryRemove(clientId, out currentRetryCount);
        }

        public TimeSpan? GetNextRetryInterval(string clientId, Exception lastException, TimeSpan remainingTime)
        {
            int baseWaitTime = 0;
            lock (this.serverBusySync)
            {
                if (lastException != null &&
                        (lastException is ServerBusyException || (lastException.InnerException != null && lastException.InnerException is ServerBusyException)))
                {
                    baseWaitTime += ClientConstants.ServerBusyBaseSleepTimeInSecs;
                }
            }

            var retryAfter = this.OnGetNextRetryInterval(clientId, lastException, remainingTime, baseWaitTime);

            // Don't retry if remaining time isn't enough.
            if (retryAfter == null || 
                remainingTime.TotalSeconds < Math.Max(retryAfter.Value.TotalSeconds, ClientConstants.TimerToleranceInSeconds))
            {
                return null;
            }

            return retryAfter;
        }

        protected int GetRetryCount(string clientId)
        {
            int retryCount;

            this.retryCounts.TryGetValue(clientId, out retryCount);

            return retryCount;
        }

        protected abstract TimeSpan? OnGetNextRetryInterval(string clientId, Exception lastException, TimeSpan remainingTime, int baseWaitTime);
    }
}
