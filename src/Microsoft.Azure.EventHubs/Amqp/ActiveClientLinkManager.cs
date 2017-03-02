﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Amqp
{
    using System;
    using System.Threading;
    using Microsoft.Azure.Amqp;

    sealed class ActiveClientLinkManager
    {
        static readonly TimeSpan SendTokenTimeout = TimeSpan.FromMinutes(1);
        static readonly TimeSpan TokenRefreshBuffer = TimeSpan.FromSeconds(300);

        readonly Timer validityTimer;
        readonly AmqpEventHubClient eventHubClient;
        readonly object syncRoot;

        ActiveClientLinkObject activeClientLink;

        public ActiveClientLinkManager(AmqpEventHubClient eventHubClient)
        {
            this.eventHubClient = eventHubClient;
            this.validityTimer = new Timer(s => OnLinkExpiration(s), this, Timeout.Infinite, Timeout.Infinite);
            this.syncRoot = new object();
        }

        public void SetActiveLink(ActiveClientLinkObject activeClientLink)
        {
            lock (this.syncRoot)
            {
                this.activeClientLink = activeClientLink;
                this.activeClientLink.LinkObject.Closed += new EventHandler(this.OnLinkClosed);
                if (this.activeClientLink.LinkObject.State == AmqpObjectState.Opened &&
                    this.activeClientLink.IsClientToken)
                {
                    this.ScheduleValidityTimer();
                }
            }
        }

        public void Close()
        {
            this.CancelValidityTimer();
        }

        static async void OnLinkExpiration(object state)
        {
            ActiveClientLinkManager thisPtr = (ActiveClientLinkManager)state;
            Fx.Assert(thisPtr.activeClientLink != null, "activeClientLink cant be null");
            Fx.Assert(thisPtr.activeClientLink.IsClientToken, "timer can't fire if the link auth is not based on a client token");

            try
            {
                //DNX_TODO: MessagingClientEtwProvider.Provider.EventWriteAmqpManageLink("Before SendToken", thisPtr.activeClientLink.LinkObject, string.Empty);

                AmqpCbsLink cbsLink = thisPtr.activeClientLink.Connection.Extensions.Find<AmqpCbsLink>();
                if (cbsLink == null)
                {
                    cbsLink = new AmqpCbsLink(thisPtr.activeClientLink.Connection);
                }

                var validTo = await cbsLink.SendTokenAsync(
                    thisPtr.eventHubClient.CbsTokenProvider,
                    thisPtr.eventHubClient.ConnectionStringBuilder.Endpoint,
                    thisPtr.activeClientLink.Audience, thisPtr.activeClientLink.EndpointUri,
                    thisPtr.activeClientLink.RequiredClaims,
                    ActiveClientLinkManager.SendTokenTimeout).ConfigureAwait(false);

                //DNX_TODO: MessagingClientEtwProvider.Provider.EventWriteAmqpManageLink("After SendToken", thisPtr.activeClientLink.LinkObject, validTo.ToString(CultureInfo.InvariantCulture));

                lock (thisPtr.syncRoot)
                {
                    thisPtr.activeClientLink.AuthorizationValidToUtc = validTo;
                    thisPtr.ScheduleValidityTimer();
                }
            }
            catch
            {
                // Failed to refresh token, no need to do anything since the server will shut the link itself
                // then we will create a new one.
                thisPtr.CancelValidityTimer();
            }
        }

        void ScheduleValidityTimer()
        {
            if (this.activeClientLink.AuthorizationValidToUtc < DateTime.UtcNow)
            {
                return;
            }

            TimeSpan interval = this.activeClientLink.AuthorizationValidToUtc.Subtract(DateTime.UtcNow);

            // Account clock skew by providing some buffer.
            interval -= TokenRefreshBuffer;
            Fx.Assert(interval >= TimeSpan.Zero, "interval can't be negative");

            interval = interval < AmqpClientConstants.ClientMinimumTokenRefreshInterval 
                ? AmqpClientConstants.ClientMinimumTokenRefreshInterval : interval;

            this.validityTimer.Change(interval, Timeout.InfiniteTimeSpan);
        }

        void OnLinkClosed(object sender, EventArgs e)
        {
            this.Close();
        }

        void CancelValidityTimer()
        {
            this.validityTimer.Change(Timeout.Infinite, Timeout.Infinite);
        }
    }
}