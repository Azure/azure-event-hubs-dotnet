// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;

    /// <summary>Represents the Azure Active Directory token provider for the Service Bus and Event Hubs.</summary>
    public class AzureActiveDirectoryTokenProvider : TokenProvider
    {
        const int DefaultCacheSize = 100;
        const string Audience = "https://eventhubs.azure.net/";

        AsyncLock authContextLock = new AsyncLock();
        AuthenticationContext authContext;
        readonly string azureSubscriptionId;
        readonly ClientCredential clientCredential;
#if !UAP10_0
        readonly ClientAssertionCertificate clientAssertionCertificate;
#endif
        readonly string clientId;
        readonly Uri redirectUri;
        readonly IPlatformParameters platformParameters;
        readonly UserIdentifier userIdentifier;

        enum AuthType
        {
            ClientCredential,
            UserPasswordCredential,
            ClientAssertionCertificate,
            InteractiveUserLogin
        }

        readonly AuthType authType;

        internal AzureActiveDirectoryTokenProvider(string azureSubscriptionId, ClientCredential credential)
            : this(authContext: null, credential: credential)
        {
            this.azureSubscriptionId = azureSubscriptionId;
        }

        internal AzureActiveDirectoryTokenProvider(AuthenticationContext authContext, ClientCredential credential)
        {
            this.clientCredential = credential;
            this.authContext = authContext;
            this.authType = AuthType.ClientCredential;
            this.clientId = clientCredential.ClientId;
        }

#if !UAP10_0
        internal AzureActiveDirectoryTokenProvider(string azureSubscriptionId, ClientAssertionCertificate clientAssertionCertificate)
            : this(authContext: null, clientAssertionCertificate: clientAssertionCertificate)
        {
            this.azureSubscriptionId = azureSubscriptionId;
        }

        internal AzureActiveDirectoryTokenProvider(AuthenticationContext authContext, ClientAssertionCertificate clientAssertionCertificate)
        {
            this.clientAssertionCertificate = clientAssertionCertificate;
            this.authContext = authContext;
            this.authType = AuthType.ClientAssertionCertificate;
            this.clientId = clientAssertionCertificate.ClientId;
        }
#endif

        internal AzureActiveDirectoryTokenProvider(string azureSubscriptionId, string clientId, Uri redirectUri, IPlatformParameters platformParameters, UserIdentifier userIdentifier)
            : this(authContext: null, clientId: clientId, redirectUri: redirectUri, platformParameters: platformParameters, userIdentifier: userIdentifier)
        {
            this.azureSubscriptionId = azureSubscriptionId;
        }

        internal AzureActiveDirectoryTokenProvider(AuthenticationContext authContext, string clientId, Uri redirectUri, IPlatformParameters platformParameters, UserIdentifier userIdentifier)
        {
            this.authContext = authContext;
            this.clientId = clientId;
            this.redirectUri = redirectUri;
            this.platformParameters = platformParameters;
            this.userIdentifier = userIdentifier;
            this.authType = AuthType.InteractiveUserLogin;
        }

        /// <summary>
        /// Gets a <see cref="SecurityToken"/> for the given audience and duration.
        /// </summary>
        /// <param name="appliesTo">The URI which the access token applies to</param>
        /// <param name="action">The request action</param>
        /// <param name="timeout">The time span that specifies the timeout value for the message that gets the security token</param>
        /// <returns><see cref="SecurityToken"/></returns>
        public override async Task<SecurityToken> GetTokenAsync(string appliesTo, string action, TimeSpan timeout)
        {
            AuthenticationResult authResult;

            if (this.authContext == null)
            {
                using (await this.authContextLock.LockAsync())
                {
                    string authority = await TenantIdProvider.GetTenantUri(this.azureSubscriptionId);
                    this.authContext = new AuthenticationContext(authority);
                }
            }

            switch (this.authType)
            {
                case AuthType.ClientCredential:
                    authResult = await this.authContext.AcquireTokenAsync(Audience, this.clientCredential);
                    break;

#if !UAP10_0
                case AuthType.ClientAssertionCertificate:
                    authResult = await this.authContext.AcquireTokenAsync(Audience, this.clientAssertionCertificate);
                    break;
#endif

                case AuthType.InteractiveUserLogin:
                    authResult = await this.authContext.AcquireTokenAsync(Audience, this.clientId, this.redirectUri, this.platformParameters, this.userIdentifier);
                    break;

                default:
                    throw new NotSupportedException();
            }

            return new JsonSecurityToken(authResult.AccessToken, appliesTo);
        }
    }
}