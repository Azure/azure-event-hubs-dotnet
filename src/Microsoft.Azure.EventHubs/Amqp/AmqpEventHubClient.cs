// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Amqp
{
    using System;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.Azure.Amqp.Sasl;
    using Microsoft.Azure.Amqp;
    using Microsoft.Azure.Amqp.Transport;
    using Microsoft.Azure.EventHubs.Amqp.Management;

    sealed class AmqpEventHubClient : EventHubClient
    {
        const string CbsSaslMechanismName = "MSSBCBS";
        AmqpServiceClient managementServiceClient; // serviceClient that handles management calls

        public AmqpEventHubClient(EventHubsConnectionStringBuilder csb)
            : base(csb)
        {
            this.ContainerId = Guid.NewGuid().ToString("N");
            this.AmqpVersion = new Version(1, 0, 0, 0);
            this.MaxFrameSize = AmqpConstants.DefaultMaxFrameSize;

            if (!string.IsNullOrWhiteSpace(csb.SharedAccessSignature))
            {
                this.TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(csb.SharedAccessSignature);
            }
            else
            {
                this.TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(csb.SasKeyName, csb.SasKey);
            }

            this.CbsTokenProvider = new TokenProviderAdapter(this);
            this.ConnectionManager = new FaultTolerantAmqpObject<AmqpConnection>(this.CreateConnectionAsync, this.CloseConnection);
        }

        internal ICbsTokenProvider CbsTokenProvider { get; }

        internal FaultTolerantAmqpObject<AmqpConnection> ConnectionManager { get; }

        internal string ContainerId { get; }

        Version AmqpVersion { get; }

        uint MaxFrameSize { get; }

        internal TokenProvider TokenProvider { get; }

        internal override EventDataSender OnCreateEventSender(string partitionId)
        {
            return new AmqpEventDataSender(this, partitionId);
        }

        protected override PartitionReceiver OnCreateReceiver(
            string consumerGroupName, string partitionId, string startOffset, bool offsetInclusive, DateTime? startTime, long? epoch, ReceiverOptions receiverOptions)
        {
            return new AmqpPartitionReceiver(
                this, consumerGroupName, partitionId, startOffset, offsetInclusive, startTime, epoch, receiverOptions);
        }

        protected override Task OnCloseAsync()
        {
            // Closing the Connection will also close all Links associated with it.
            return this.ConnectionManager.CloseAsync();
        }

        protected override async Task<EventHubRuntimeInformation> OnGetRuntimeInformationAsync()
        {
            var serviceClient = this.GetManagementServiceClient();
            var eventHubRuntimeInformation = await serviceClient.GetRuntimeInformationAsync().ConfigureAwait(false);

            return eventHubRuntimeInformation;
        }

        protected override async Task<EventHubPartitionRuntimeInformation> OnGetPartitionRuntimeInformationAsync(string partitionId)
        {
            var serviceClient = this.GetManagementServiceClient();
            var eventHubPartitionRuntimeInformation = await serviceClient.
                GetPartitionRuntimeInformationAsync(partitionId).ConfigureAwait(false);

            return eventHubPartitionRuntimeInformation;
        }

        internal AmqpServiceClient GetManagementServiceClient()
        {
            if (this.managementServiceClient == null)
            {
                lock (ThisLock)
                {
                    if (this.managementServiceClient == null)
                    {
                        this.managementServiceClient = new AmqpServiceClient(this, AmqpClientConstants.ManagementAddress);
                    }

                    Fx.Assert(string.Equals(this.managementServiceClient.Address, AmqpClientConstants.ManagementAddress, StringComparison.OrdinalIgnoreCase),
                        "The address should match the address of managementServiceClient");
                }
            }

            return this.managementServiceClient;
        }

        internal static AmqpSettings CreateAmqpSettings(
            Version amqpVersion,
            bool useSslStreamSecurity,
            bool hasTokenProvider,
            string sslHostName = null,
            bool useWebSockets = false,
            bool sslStreamUpgrade = false,
            NetworkCredential networkCredential = null,
            bool forceTokenProvider = true)
        {
            var settings = new AmqpSettings();
            if (useSslStreamSecurity && !useWebSockets && sslStreamUpgrade)
            {
                var tlsSettings = new TlsTransportSettings
                {
                    TargetHost = sslHostName
                };

                var tlsProvider = new TlsTransportProvider(tlsSettings);
                tlsProvider.Versions.Add(new AmqpVersion(amqpVersion));
                settings.TransportProviders.Add(tlsProvider);
            }

            if (hasTokenProvider || networkCredential != null)
            {
                var saslProvider = new SaslTransportProvider();
                saslProvider.Versions.Add(new AmqpVersion(amqpVersion));
                settings.TransportProviders.Add(saslProvider);

                if (forceTokenProvider)
                {
                    saslProvider.AddHandler(new SaslAnonymousHandler(CbsSaslMechanismName));
                }
                else if (networkCredential != null)
                {
                    var plainHandler = new SaslPlainHandler
                    {
                        AuthenticationIdentity = networkCredential.UserName,
                        Password = networkCredential.Password
                    };
                    saslProvider.AddHandler(plainHandler);
                }
                else
                {
                    // old client behavior: keep it for validation only
                    saslProvider.AddHandler(new SaslExternalHandler());
                }
            }

            var amqpProvider = new AmqpTransportProvider();
            amqpProvider.Versions.Add(new AmqpVersion(amqpVersion));
            settings.TransportProviders.Add(amqpProvider);

            return settings;
        }

        static TransportSettings CreateTcpTlsTransportSettings(string hostName, int port)
        {
            TcpTransportSettings tcpSettings = new TcpTransportSettings
            {
                Host = hostName,
                Port = port < 0 ? AmqpConstants.DefaultSecurePort : port,
                ReceiveBufferSize = AmqpConstants.TransportBufferSize,
                SendBufferSize = AmqpConstants.TransportBufferSize
            };

            TlsTransportSettings tlsSettings = new TlsTransportSettings(tcpSettings)
            {
                TargetHost = hostName,
            };

            return tlsSettings;
        }

        static TransportSettings CreateWebSocketsTransportSettings(string hostName)
        {
            var uriBuilder = new UriBuilder(hostName)
            {
                Path = AmqpClientConstants.WebSocketsPathSuffix,
                Scheme = AmqpClientConstants.UriSchemeWss,
                Port = -1 // Port will be assigned on transport listener.
            };
            var ts = new WebSocketTransportSettings()
            {
                Uri = uriBuilder.Uri
            };

            return ts;
        }

        static AmqpConnectionSettings CreateAmqpConnectionSettings(uint maxFrameSize, string containerId, string hostName)
        {
            var connectionSettings = new AmqpConnectionSettings
            {
                MaxFrameSize = maxFrameSize,
                ContainerId = containerId,
                HostName = hostName
            };

            ClientInfo.Add(connectionSettings);
            return connectionSettings;
        }

        async Task<AmqpConnection> CreateConnectionAsync(TimeSpan timeout)
        {
            string hostName = this.ConnectionStringBuilder.Endpoint.Host;
            int port = this.ConnectionStringBuilder.Endpoint.Port;
            bool useWebSockets = this.ConnectionStringBuilder.TransportType == Microsoft.Azure.EventHubs.TransportType.AmqpWebSockets;

            var timeoutHelper = new TimeoutHelper(timeout);
            var amqpSettings = CreateAmqpSettings(
                amqpVersion: this.AmqpVersion,
                useSslStreamSecurity: true,
                hasTokenProvider: true,
                useWebSockets: useWebSockets);

            TransportSettings tpSettings = null;
            if (useWebSockets)
            {
                tpSettings = CreateWebSocketsTransportSettings(hostName);
            }
            else
            {
                tpSettings = CreateTcpTlsTransportSettings(hostName, port);
            }

            var initiator = new AmqpTransportInitiator(amqpSettings, tpSettings);
            var transport = await initiator.ConnectTaskAsync(timeoutHelper.RemainingTime()).ConfigureAwait(false);

            var connectionSettings = CreateAmqpConnectionSettings(this.MaxFrameSize, this.ContainerId, hostName);
            var connection = new AmqpConnection(transport, amqpSettings, connectionSettings);
            await connection.OpenAsync(timeoutHelper.RemainingTime()).ConfigureAwait(false);

            // Always create the CBS Link + Session
            var cbsLink = new AmqpCbsLink(connection);
            if (connection.Extensions.Find<AmqpCbsLink>() == null)
            {
                connection.Extensions.Add(cbsLink);
            }

            return connection;
        }

        void CloseConnection(AmqpConnection connection)
        {
            connection.SafeClose();
        }

        /// <summary>
        /// Provides an adapter from TokenProvider to ICbsTokenProvider for AMQP CBS usage.
        /// </summary>
        sealed class TokenProviderAdapter : ICbsTokenProvider
        {
            readonly AmqpEventHubClient eventHubClient;

            public TokenProviderAdapter(AmqpEventHubClient eventHubClient)
            {
                Fx.Assert(eventHubClient != null, "tokenProvider cannot be null");
                this.eventHubClient = eventHubClient;
            }

            public async Task<CbsToken> GetTokenAsync(Uri namespaceAddress, string appliesTo, string[] requiredClaims)
            {
                string claim = requiredClaims?.FirstOrDefault();
                var tokenProvider = this.eventHubClient.TokenProvider;
                var timeout = this.eventHubClient.ConnectionStringBuilder.OperationTimeout;
                var token = await tokenProvider.GetTokenAsync(appliesTo, claim, timeout).ConfigureAwait(false);
                return new CbsToken(token.TokenValue, CbsConstants.ServiceBusSasTokenType, token.ExpiresAtUtc);
            }
        }
    }
}