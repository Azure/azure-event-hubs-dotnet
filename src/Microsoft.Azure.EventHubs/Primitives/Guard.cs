// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Primitives
{
    static class Guard
    {
        internal static void ArgumentNotNull(string argumentName, object value)
        {
            if (value == null)
            {
                Fx.Exception.ArgumentNull(argumentName);
            }
        }

        internal static void ArgumentNotNullOrEmpty(string argumentName, string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                Fx.Exception.ArgumentNull(argumentName);
            }
        }
    }
}
