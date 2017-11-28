// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs
{
    using System;
    using System.Collections.ObjectModel;
    using System.IdentityModel.Tokens;
    using System.IdentityModel.Tokens.Jwt;

    class JsonSecurityToken : SecurityToken
    {
        JwtSecurityToken internalToken;

        public JsonSecurityToken(string rawToken, string audience)
        {
            this.internalToken = new JwtSecurityToken(rawToken);
            this.tokenType = ClientConstants.JsonWebTokenType;
            this.token = rawToken;
            this.expiresAtUtc = this.internalToken.ValidTo;
            this.audience = audience;
        }
    }
}
