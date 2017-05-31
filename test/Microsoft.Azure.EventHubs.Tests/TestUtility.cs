// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Tests
{
    using System;
    using System.Diagnostics;
    using Microsoft.Azure.EventHubs;

    static class TestUtility
    {
        static TestUtility()
        {
            var ehConnectionString = Environment.GetEnvironmentVariable(TestConstants.EventHubsConnectionStringEnvironmentVariableName);
            var storageConnectionString = Environment.GetEnvironmentVariable(TestConstants.StorageConnectionStringEnvironmentVariableName);

            if (string.IsNullOrWhiteSpace(ehConnectionString))
            {
                throw new InvalidOperationException($"'{TestConstants.EventHubsConnectionStringEnvironmentVariableName}' environment variable was not found!");
            }

            if (string.IsNullOrWhiteSpace(storageConnectionString))
            {
                throw new InvalidOperationException($"'{TestConstants.StorageConnectionStringEnvironmentVariableName}' environment variable was not found!");
            }

            StorageConnectionString = storageConnectionString;

            // Validate the connection string
            var ehCsb = new EventHubsConnectionStringBuilder(ehConnectionString);
            if (ehCsb.EntityPath == null)
            {
                ehCsb.EntityPath = TestConstants.DefultEventHubName;
            }

            // Update operation timeout on ConnectionStringBuilder.
            ehCsb.OperationTimeout = TimeSpan.FromSeconds(15);
            EventHubsConnectionString = ehCsb.ToString();
        }

        internal static string EventHubsConnectionString { get; }

        internal static string StorageConnectionString { get; }

        internal static string GetEntityConnectionString(string entityName)
        {
            // If the entity name is populated in the connection string, it will be overridden.
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubsConnectionString)
            {
                EntityPath = entityName
            };
            return connectionStringBuilder.ToString();
        }

        internal static void Log(string message)
        {
            var formattedMessage = $"{DateTime.Now.TimeOfDay}: {message}";
            Debug.WriteLine(formattedMessage);
            Console.WriteLine(formattedMessage);
        }
    }
}