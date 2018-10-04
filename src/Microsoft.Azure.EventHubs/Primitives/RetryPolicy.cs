// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using System.Collections.Concurrent;
    using System.Net.Sockets;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents an abstraction for retrying messaging operations. Users should not 
    /// implement this class, and instead should use one of the provided implementations.
    /// </summary>
    public abstract class RetryPolicy
    {
        const int DefaultRetryMaxCount = 10;

        static readonly TimeSpan DefaultRetryMinBackoff = TimeSpan.Zero;
        static readonly TimeSpan DefaultRetryMaxBackoff = TimeSpan.FromSeconds(30);

        object serverBusySync;

        /// <summary></summary>
        protected RetryPolicy()
        {
            this.serverBusySync = new Object();
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
            else if (exception is TaskCanceledException)
            {
                if (exception.InnerException != null)
                {
                    return IsRetryableException(exception.InnerException);
                }

                return true;
            }

            // Flatten AggregateException
            else if (exception is AggregateException)
            {
                var fltAggException = (exception as AggregateException).Flatten();
                if (fltAggException.InnerException != null)
                {
                    return IsRetryableException(fltAggException.InnerException);
                }

                return false;
            }

            // Other retryable exceptions here.
            else if (exception is OperationCanceledException ||
                exception is SocketException)
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
        /// <param name="lastException"></param>
        /// <param name="remainingTime"></param>
        /// <param name="baseWaitTime"></param>
        /// <param name="retryCount"></param>
        /// <returns></returns>
        protected abstract TimeSpan? OnGetNextRetryInterval(Exception lastException, TimeSpan remainingTime, int baseWaitTime, int retryCount);

        /// <summary>
        /// Gets the timespan for the next retry operation.
        /// </summary>
        /// <param name="lastException">The last exception that was thrown</param>
        /// <param name="remainingTime">Remaining time for the cumulative timeout</param>
        /// <param name="retryCount">Current retry count</param>
        /// <returns></returns>
        public TimeSpan? GetNextRetryInterval(Exception lastException, TimeSpan remainingTime, int retryCount)
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

            var retryAfter = this.OnGetNextRetryInterval(lastException, remainingTime, baseWaitTime, retryCount);

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