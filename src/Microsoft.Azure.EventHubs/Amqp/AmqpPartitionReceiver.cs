﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Amqp
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Amqp;
    using Microsoft.Azure.Amqp.Encoding;
    using Microsoft.Azure.Amqp.Framing;

    class AmqpPartitionReceiver : PartitionReceiver
    {
        readonly object receivePumpLock;
        readonly ActiveClientLinkManager clientLinkManager;
        IPartitionReceiveHandler receiveHandler;
        CancellationTokenSource receivePumpCancellationSource;
        Task receivePumpTask;

        public AmqpPartitionReceiver(
            AmqpEventHubClient eventHubClient,
            string consumerGroupName,
            string partitionId,
            EventPosition eventPosition,
            long? epoch,
            ReceiverOptions receiverOptions)
            : base(eventHubClient, consumerGroupName, partitionId, eventPosition, epoch, receiverOptions)
        {
            string entityPath = eventHubClient.ConnectionStringBuilder.EntityPath;
            this.Path = $"{entityPath}/ConsumerGroups/{consumerGroupName}/Partitions/{partitionId}";
            this.ReceiveLinkManager = new FaultTolerantAmqpObject<ReceivingAmqpLink>(this.CreateLinkAsync, this.CloseSession);
            this.receivePumpLock = new object();
            this.clientLinkManager = new ActiveClientLinkManager((AmqpEventHubClient)this.EventHubClient);
        }

        string Path { get; }

        FaultTolerantAmqpObject<ReceivingAmqpLink> ReceiveLinkManager { get; }

        protected override Task OnCloseAsync()
        {
            // Close any ReceiveHandler (this is safe if there is none) and the ReceiveLinkManager in parallel.
            this.ReceiveHandlerClose();
            this.clientLinkManager.Close();
            return this.ReceiveLinkManager.CloseAsync();
        }

        protected override async Task<IList<EventData>> OnReceiveAsync(int maxMessageCount, TimeSpan waitTime)
        {
            bool shouldRetry;

            var timeoutHelper = new TimeoutHelper(waitTime, true);

            do
            {
                shouldRetry = false;

                try
                {
                    try
                    {
                        ReceivingAmqpLink receiveLink = await this.ReceiveLinkManager.GetOrCreateAsync(timeoutHelper.RemainingTime()).ConfigureAwait(false);
                        IEnumerable<AmqpMessage> amqpMessages = null;
                        bool hasMessages = await Task.Factory.FromAsync(
                            (c, s) => receiveLink.BeginReceiveMessages(maxMessageCount, timeoutHelper.RemainingTime(), c, s),
                            a => receiveLink.EndReceiveMessages(a, out amqpMessages),
                            this).ConfigureAwait(false);

                        if (receiveLink.TerminalException != null)
                        {
                            throw receiveLink.TerminalException;
                        }

                        this.RetryPolicy.ResetRetryCount(this.ClientId);

                        if (hasMessages && amqpMessages != null)
                        {
                            IList<EventData> eventDatas = null;
                            foreach (var amqpMessage in amqpMessages)
                            {
                                if (eventDatas == null)
                                {
                                    eventDatas = new List<EventData>();
                                }

                                receiveLink.DisposeDelivery(amqpMessage, true, AmqpConstants.AcceptedOutcome);
                                eventDatas.Add(AmqpMessageConverter.AmqpMessageToEventData(amqpMessage));
                            }

                            return eventDatas;
                        }
                    }
                    catch (AmqpException amqpException)
                    {
                        throw AmqpExceptionHelper.ToMessagingContract(amqpException.Error);
                    }
                }
                catch (Exception ex)
                {
                    // Evaluate retry condition?
                    this.RetryPolicy.IncrementRetryCount(this.ClientId);
                    TimeSpan? retryInterval = this.RetryPolicy.GetNextRetryInterval(this.ClientId, ex, timeoutHelper.RemainingTime());
                    if (retryInterval != null)
                    {
                        await Task.Delay(retryInterval.Value).ConfigureAwait(false);
                        shouldRetry = true;
                    }
                    else
                    {
                        // Handle System.TimeoutException explicitly.
                        // We don't really want to to throw TimeoutException on this call.
                        if (ex is TimeoutException)
                        {
                            break;
                        }

                        throw;
                    }
                }
            } while (shouldRetry);

            // No messages to deliver.
            return null;
        }

        protected override void OnSetReceiveHandler(IPartitionReceiveHandler newReceiveHandler, bool invokeWhenNoEvents)
        {
            lock (this.receivePumpLock)
            {
                if (newReceiveHandler != null)
                {
                    if (this.receiveHandler != null)
                    {
                        // Notify existing handler first (but don't wait).
                        Task.Run(() =>
                            this.receiveHandler.ProcessErrorAsync(new OperationCanceledException("New handler has registered for this receiver.")))
                            .ContinueWith(t =>
                                t.Exception.Handle(ex =>
                                {
                                    // We omit any failures from ProcessErrorAsync
                                    return true;
                                }), TaskContinuationOptions.OnlyOnFaulted);
                    }

                    this.receiveHandler = newReceiveHandler;

                    // We have a new receiveHandler, ensure pump is running.
                    if (this.receivePumpTask == null)
                    {
                        this.receivePumpCancellationSource = new CancellationTokenSource();
                        this.receivePumpTask = this.ReceivePumpAsync(this.receivePumpCancellationSource.Token, invokeWhenNoEvents);
                    }
                }
                else
                {
                    // newReceiveHandler == null, so this is an unregister call, ensure pump is shut down.
                    if (this.receivePumpTask != null)
                    {
                        this.ReceiveHandlerClose();
                    }

                    this.receiveHandler = null;
                }
            }
        }

        async Task<ReceivingAmqpLink> CreateLinkAsync(TimeSpan timeout)
        {
            var amqpEventHubClient = ((AmqpEventHubClient)this.EventHubClient);

            // We won't use remaining timeout during create session call.
            // For large or small operation timeout values using remaining time won't make any sense.
            var timeoutHelper = new TimeoutHelper(TimeSpan.FromSeconds(AmqpClientConstants.AmqpSessionTimeoutInSeconds));

            AmqpConnection connection = await amqpEventHubClient.ConnectionManager.GetOrCreateAsync(timeoutHelper.RemainingTime()).ConfigureAwait(false);

            // Authenticate over CBS
            var cbsLink = connection.Extensions.Find<AmqpCbsLink>();

            ICbsTokenProvider cbsTokenProvider = amqpEventHubClient.CbsTokenProvider;
            Uri address = new Uri(amqpEventHubClient.ConnectionStringBuilder.Endpoint, this.Path);
            string audience = address.AbsoluteUri;
            string resource = address.AbsoluteUri;
            var expiresAt = await cbsLink.SendTokenAsync(cbsTokenProvider, address, audience, resource, new[] { ClaimConstants.Listen }, timeoutHelper.RemainingTime()).ConfigureAwait(false);

            AmqpSession session = null;
            try
            {
                // Create our Session
                var sessionSettings = new AmqpSessionSettings { Properties = new Fields() };
                session = connection.CreateSession(sessionSettings);
                await session.OpenAsync(timeoutHelper.RemainingTime()).ConfigureAwait(false);

                FilterSet filterMap = null;
                var filters = this.CreateFilters();
                if (filters != null && filters.Count > 0)
                {
                    filterMap = new FilterSet();
                    foreach (var filter in filters)
                    {
                        filterMap.Add(filter.DescriptorName, filter);
                    }
                }

                // Create our Link
                var linkSettings = new AmqpLinkSettings();
                linkSettings.Role = true;
                linkSettings.TotalLinkCredit = (uint)this.PrefetchCount;
                linkSettings.AutoSendFlow = this.PrefetchCount > 0;
                linkSettings.AddProperty(AmqpClientConstants.EntityTypeName, (int)MessagingEntityType.ConsumerGroup);
                linkSettings.Source = new Source { Address = address.AbsolutePath, FilterSet = filterMap };
                linkSettings.Target = new Target { Address = this.ClientId };
                linkSettings.SettleType = SettleMode.SettleOnSend;

                // Receiver metrics enabled?
                if (this.ReceiverRuntimeMetricEnabled)
                {
                    linkSettings.DesiredCapabilities = new Multiple<AmqpSymbol>(new List<AmqpSymbol>
                        {
                            AmqpClientConstants.EnableReceiverRuntimeMetricName
                        });
                }

                if (this.Epoch.HasValue)
                {
                    linkSettings.AddProperty(AmqpClientConstants.AttachEpoch, this.Epoch.Value);
                }

                if (!string.IsNullOrWhiteSpace(this.Identifier))
                {
                    linkSettings.AddProperty(AmqpClientConstants.ReceiverIdentifierName, this.Identifier);
                }

                var link = new ReceivingAmqpLink(linkSettings);
                linkSettings.LinkName = $"{amqpEventHubClient.ContainerId};{connection.Identifier}:{session.Identifier}:{link.Identifier}";
                link.AttachTo(session);

                await link.OpenAsync(timeoutHelper.RemainingTime()).ConfigureAwait(false);
                var activeClientLink = new ActiveClientLink(
                    link,
                    audience, // audience
                    this.EventHubClient.ConnectionStringBuilder.Endpoint.AbsoluteUri, // endpointUri
                    new[] { ClaimConstants.Listen },
                    true,
                    expiresAt);

                this.clientLinkManager.SetActiveLink(activeClientLink);

                return link;
            }
            catch
            {
                // Cleanup any session (and thus link) in case of exception.
                session?.Abort();
                throw;
            }
        }

        void CloseSession(ReceivingAmqpLink link)
        {
            link.Session.SafeClose();
        }

        IList<AmqpDescribed> CreateFilters()
        {
            List<AmqpDescribed> filterMap = null;

            if (this.EventPosition != null)
            {
                filterMap = new List<AmqpDescribed>();
                filterMap.Add(new AmqpSelectorFilter(this.EventPosition.GetExpression()));
            }

            return filterMap;
        }

        async Task ReceivePumpAsync(CancellationToken cancellationToken, bool invokeWhenNoEvents)
        {
            try
            {
                // Loop until pump is shutdown or an error is hit.
                while (!cancellationToken.IsCancellationRequested)
                {
                    IEnumerable<EventData> receivedEvents;

                    try
                    {
                        int batchSize;
                        lock (this.receivePumpLock)
                        {
                            if (this.receiveHandler == null)
                            {
                                // Pump has been shutdown, nothing more to do.
                                return;
                            }
                            batchSize = receiveHandler.MaxBatchSize;
                        }

                        receivedEvents = await this.ReceiveAsync(batchSize).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        EventHubsEventSource.Log.ReceiveHandlerExitingWithError(this.ClientId, this.PartitionId, e.Message);
                        await this.ReceiveHandlerProcessErrorAsync(e).ConfigureAwait(false);

                        // Avoid tight loop if Receieve call keeps faling.
                        await Task.Delay(100).ConfigureAwait(false);

                        continue;
                    }

                    if (invokeWhenNoEvents || receivedEvents != null)
                    {
                        try
                        {
                            await this.ReceiveHandlerProcessEventsAsync(receivedEvents).ConfigureAwait(false);
                        }
                        catch (Exception userCodeError)
                        {
                            EventHubsEventSource.Log.ReceiveHandlerExitingWithError(this.ClientId, this.PartitionId, userCodeError.Message);
                            await this.ReceiveHandlerProcessErrorAsync(userCodeError).ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // This should never throw
                EventHubsEventSource.Log.ReceiveHandlerExitingWithError(this.ClientId, this.PartitionId, ex.Message);
                Environment.FailFast(ex.ToString());
            }
        }

        // Encapsulates taking the receivePumpLock, checking this.receiveHandler for null,
        // calls this.receiveHandler.CloseAsync (starting this operation inside the receivePumpLock).
        void ReceiveHandlerClose()
        {
            lock (this.receivePumpLock)
            {
                if (this.receiveHandler != null)
                {
                    if (this.receivePumpTask != null)
                    {
                        this.receivePumpCancellationSource.Cancel();
                        this.receivePumpCancellationSource.Dispose();
                        this.receivePumpCancellationSource = null;
                        this.receivePumpTask = null;
                    }

                    this.receiveHandler = null;
                }
            }
        }

        // Encapsulates taking the receivePumpLock, checking this.receiveHandler for null,
        // calls this.receiveHandler.ProcessErrorAsync (starting this operation inside the receivePumpLock).
        Task ReceiveHandlerProcessErrorAsync(Exception error)
        {
            Task processErrorTask = null;
            lock (this.receivePumpLock)
            {
                if (this.receiveHandler != null)
                {
                    processErrorTask = this.receiveHandler.ProcessErrorAsync(error);
                }
            }

            return processErrorTask ?? Task.FromResult(0);
        }

        // Encapsulates taking the receivePumpLock, checking this.receiveHandler for null,
        // calls this.receiveHandler.ProcessErrorAsync (starting this operation inside the receivePumpLock).
        Task ReceiveHandlerProcessEventsAsync(IEnumerable<EventData> eventDatas)
        {
            Task processEventsTask = null;

            lock (this.receivePumpLock)
            {
                if (this.receiveHandler != null)
                {
                    processEventsTask = this.receiveHandler.ProcessEventsAsync(eventDatas);
                }
            }

            return processEventsTask ?? Task.FromResult(0);
        }
    }
}
