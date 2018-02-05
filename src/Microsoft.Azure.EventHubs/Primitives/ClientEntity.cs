// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Contract for all client entities with Open-Close/Abort state m/c
    /// main-purpose: closeAll related entities
    /// </summary>
    public abstract class ClientEntity
    {
        static int nextId;

        RetryPolicy retryPolicy;

        /// <summary></summary>
        /// <param name="clientId"></param>
        protected ClientEntity(string clientId)
        {
            this.ClientId = clientId;
        }

        /// <summary>
        /// Gets the client ID.
        /// </summary>
        public string ClientId
        {
            get; private set;
        }

        /// <summary>
        /// Gets the <see cref="EventHubs.RetryPolicy"/> for the ClientEntity.
        /// </summary>
        public RetryPolicy RetryPolicy
        {
            get
            {
                return this.retryPolicy;
            }

            set
            {
                this.retryPolicy = value;
                this.OnRetryPolicyUpdate();
            }
        }

        /// <summary>
        /// Closes the ClientEntity.
        /// </summary>
        /// <returns>The asynchronous operation</returns>
        public abstract Task CloseAsync();

        /// <summary>
        /// Closes the ClientEntity.
        /// </summary>
        public void Close()
        {
            this.CloseAsync().GetAwaiter().GetResult();
        }

        /// <summary></summary>
        /// <returns></returns>
        protected static long GetNextId()
        {
            return Interlocked.Increment(ref nextId);
        }

        /// <summary>
        /// Derived entity to override for retry policy updates.
        /// </summary>
        protected virtual void OnRetryPolicyUpdate()
        {
            // NOOP
        }
    }
}