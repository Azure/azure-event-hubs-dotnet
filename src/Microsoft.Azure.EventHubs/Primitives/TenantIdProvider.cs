// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Threading.Tasks;

    class TenantIdProvider
    {
        const string ArmApiVersion = "2016-06-01";
        static readonly HttpClient httpClient;

        static TenantIdProvider()
        {
            httpClient = new HttpClient();
        }

        public static async Task<string> GetTenantUri(string subscriptionId, string armEndpoint = "https://management.azure.com/")
        {
            Uri armUri = new Uri($"{armEndpoint}subscriptions/{subscriptionId}?api-version={ArmApiVersion}");

            var request = new HttpRequestMessage
            {
                RequestUri = armUri,
                Method = HttpMethod.Get,
            };

            string tenantUri;

            using (var response = await httpClient.SendAsync(request))
            {
                string responseContent = await response.Content.ReadAsStringAsync();

                var authHeader = response.Headers.GetValues("WWW-Authenticate").FirstOrDefault();
                if (string.IsNullOrEmpty(authHeader))
                {
                    throw new InvalidOperationException(string.Format(Resources.TenantIdLookupFailed, "'WWW-Authenticate' cannot be located from the authHeader"));
                }

                /* e.g.
                WWW-Authenticate: Bearer authorization_uri="https://login.windows.net/b06452b1-437c-4d7d-8363-54769cb614fa", error="invalid_token", error_description="The access token is from the wrong issuer.
                It must match the tenant associated with this subscription. Please use correct authority to get the token."
                */
                string authUri = "authorization_uri";
                int indexOfAuthUri = authHeader.IndexOf(authUri, StringComparison.OrdinalIgnoreCase);

                if (indexOfAuthUri == -1)
                {
                    throw new InvalidOperationException(string.Format(Resources.TenantIdLookupFailed, "'authorization_uri' cannot be located from the authHeader"));
                }

                int indexOfTenantUri = authHeader.IndexOf("\"", indexOfAuthUri, StringComparison.OrdinalIgnoreCase) + 1;
                tenantUri = authHeader.Substring(indexOfTenantUri, authHeader.IndexOf("\"", indexOfTenantUri + 1, StringComparison.OrdinalIgnoreCase) - indexOfTenantUri);
            }

            return tenantUri;
        }
    }
}
