// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using System.Collections.Concurrent;

    /// <summary>
    /// Represents an abstraction for retrying messaging operations. Users should not 
    /// implement this class, and instead should use one of the provided implementations.
    /// </summary>
    public abstract class RetryPolicy
    {
        const int DefaultRetryMaxCount = 10;

        static readonly TimeSpan DefaultRetryMinBackoff = TimeSpan.Zero;
        static readonly TimeSpan DefaultRetryMaxBackoff = TimeSpan.FromSeconds(30);

        // Same retry policy may be used by multiple senders and receivers.
        // Because of this we keep track of retry counters in a concurrent dictionary.
        ConcurrentDictionary<String, int> retryCounts;
        object serverBusySync;

        /// <summary></summary>
        protected RetryPolicy()
        {
            this.retryCounts = new ConcurrentDictionary<string, int>();
            this.serverBusySync = new Object();
        }

        /// <summary>
        /// Increases the retry count.
        /// </summary>
        /// <param name="clientId">The <see cref="ClientEntity.ClientId"/> associated with the operation to retry</param>
        public void IncrementRetryCount(string clientId)
        {
            this.retryCounts.AddOrUpdate(clientId, 1, (k, v) => v + 1);
        }

        /// <summary>
        /// Resets the retry count to zero.
        /// </summary>
        /// <param name="clientId">The <see cref="ClientEntity.ClientId"/> associated with the operation to retry</param>
        public void ResetRetryCount(string clientId)
        {
            int currentRetryCount;
            this.retryCounts.TryRemove(clientId, out currentRetryCount);
        }

        /// <summary>
        /// Determines whether or not the exception can be retried.
        /// </summary>
        /// <param name="exception"></param>
        /// <returns>A bool indicating whether or not the operation can be retried.</returns>
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

        /// <summary>
        /// Returns the default retry policy, <see cref="RetryExponential"/>.
        /// </summary>
        public static RetryPolicy Default
        {
            get
            {
                return new RetryExponential(DefaultRetryMinBackoff, DefaultRetryMaxBackoff, DefaultRetryMaxCount);
            }
        }

        /// <summary>
        /// Returns the default retry policy, <see cref="NoRetry"/>.
        /// </summary>
        public static RetryPolicy NoRetry
        {
            get
            {
                return new RetryExponential(TimeSpan.Zero, TimeSpan.Zero, 0);
            }
        }

        /// <summary></summary>
        /// <param name="clientId"></param>
        /// <returns></returns>
        protected int GetRetryCount(string clientId)
        {
            int retryCount;

            this.retryCounts.TryGetValue(clientId, out retryCount);

            return retryCount;
        }

        /// <summary></summary>
        /// <param name="clientId"></param>
        /// <param name="lastException"></param>
        /// <param name="remainingTime"></param>
        /// <param name="baseWaitTime"></param>
        /// <returns></returns>
        protected abstract TimeSpan? OnGetNextRetryInterval(String clientId, Exception lastException, TimeSpan remainingTime, int baseWaitTime);

        /// <summary>
        /// Gets the timespan for the next retry operation.
        /// </summary>
        /// <param name="clientId">The <see cref="ClientEntity.ClientId"/> associated with the operation to retry</param>
        /// <param name="lastException">The last exception that was thrown</param>
        /// <param name="remainingTime">Remaining time for the cumulative timeout</param>
        /// <returns></returns>
        public TimeSpan? GetNextRetryInterval(string clientId, Exception lastException, TimeSpan remainingTime)
        {
            int baseWaitTime = 0;
            lock(this.serverBusySync)
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

        /// <summary>Creates a new copy of the current <see cref="RetryPolicy" /> and clones it into a new instance.</summary>
        /// <returns>A new copy of <see cref="RetryPolicy" />.</returns>
        public abstract RetryPolicy Clone();
    }
}