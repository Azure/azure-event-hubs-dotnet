// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;

namespace Microsoft.Azure.EventHubs
{
    using Microsoft.Azure.EventHubs.Core;
    using System;
    using System.Collections.Generic;
    using System.Linq;
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
        /// Gets a list of currently registered plugins for this Client.
        /// </summary>
        public virtual ConcurrentDictionary<string, EventHubsPlugin> RegisteredPlugins { get; }
            = new ConcurrentDictionary<string, EventHubsPlugin>();

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
        /// Registers a <see cref="EventHubsPlugin"/> to be used with this client.
        /// </summary>
        public virtual void RegisterPlugin(EventHubsPlugin eventHubPlugin)
        {
            if (eventHubPlugin == null)
            {
                throw new ArgumentNullException(nameof(eventHubPlugin), Resources.ArgumentNullOrWhiteSpace.FormatForUser(nameof(eventHubPlugin)));
            }
            if (this.RegisteredPlugins.Any(p => p.Key == eventHubPlugin.Name))
            {
                throw new ArgumentException(nameof(eventHubPlugin), Resources.PluginAlreadyRegistered.FormatForUser(nameof(eventHubPlugin.Name)));
            }
            if (!this.RegisteredPlugins.TryAdd(eventHubPlugin.Name, eventHubPlugin))
            {
                throw new ArgumentException(nameof(eventHubPlugin), Resources.PluginRegistrationFailed.FormatForUser(nameof(eventHubPlugin.Name)));
            }
        }

        /// <summary>
        /// Unregisters a <see cref="EventHubsPlugin"/>.
        /// </summary>
        /// <param name="eventHubPlugin">The <see cref="EventHubsPlugin.Name"/> of the plugin to be unregistered.</param>
        public virtual void UnregisterPlugin(string eventHubPlugin)
        {
            if (this.RegisteredPlugins == null)
            {
                return;
            }
            if (string.IsNullOrWhiteSpace(eventHubPlugin))
            {
                throw new ArgumentNullException(nameof(eventHubPlugin), Resources.ArgumentNullOrWhiteSpace.FormatForUser(nameof(eventHubPlugin)));
            }

            this.RegisteredPlugins.TryRemove(eventHubPlugin, out EventHubsPlugin plugin);
        }

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