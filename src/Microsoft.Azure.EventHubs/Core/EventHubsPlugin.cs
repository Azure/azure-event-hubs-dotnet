// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Core
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    ///     This class provides methods that can be overridden to manipulate messages for custom plugin functionality.
    /// </summary>
    public abstract class EventHubsPlugin
    {
        /// <summary>
        ///     Gets the name of the <see cref="EventHubsPlugin" />.
        /// </summary>
        /// <remarks>This name is used to identify the plugin, and prevent a plugin from being registered multiple times.</remarks>
        public abstract string Name { get; }

        /// <summary>
        ///     Determines whether or an exception in the plugin should prevent a send or receive operation.
        /// </summary>
        public virtual bool ShouldContinueOnException => false;

        /// <summary>
        ///     This operation is called before an event is sent.
        /// </summary>
        /// <param name="eventData">The <see cref="EventData" /> to be modified by the plugin</param>
        /// <returns>The modified event <see cref="EventData" /></returns>
        public virtual Task<EventData> BeforeEventSend(EventData eventData)
        {
            return Task.FromResult(eventData);
        }

        /// <summary>
        ///     This operation is called after a event collection is received, but before it is returned to the client.
        /// </summary>
        /// <param name="eventsData">The <see cref="EventData"/> collection of events to be modified by the plugin</param>
        /// <returns>The modified collection of events <see cref="EventData"/></returns>
        public virtual Task<IEnumerable<EventData>> AfterEventsReceive(IEnumerable<EventData> eventsData)
        {
            return Task.FromResult(eventsData);
        }
    }
}