﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs
{
    /// <summary>
    /// Provides interface definition of a token provider.
    /// </summary>
    public interface ITokenProvider
    {
        /// <summary>
        /// Gets a <see cref="SecurityToken"/>.
        /// </summary>
        /// <param name="appliesTo">The URI which the access token applies to</param>
        /// <param name="action">The request action</param>
        /// <param name="timeout">The time span that specifies the timeout value for the message that gets the security token</param>
        /// <returns><see cref="SecurityToken"/></returns>
        Task<SecurityToken> GetTokenAsync(string appliesTo, string action, TimeSpan timeout);
    }
}
