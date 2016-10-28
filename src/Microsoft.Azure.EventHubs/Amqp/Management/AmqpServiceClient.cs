// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Amqp.Management
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using Microsoft.Azure.Amqp;
    using Microsoft.Azure.Amqp.Encoding;
    using Microsoft.Azure.Amqp.Framing;
    using Microsoft.Azure.Amqp.Serialization;

    class AmqpServiceClient<T> : ClientEntity
    {
        static readonly string[] RequiredClaims = { ClaimConstants.Manage, ClaimConstants.Listen };
        readonly AmqpEventHubClient eventHubClient;
        readonly FaultTolerantAmqpObject<RequestResponseAmqpLink> link;
        readonly ActiveClientLinkManager clientLinkManager;

        public AmqpServiceClient(AmqpEventHubClient eventHubClient, string address)
            : base(nameof(AmqpServiceClient<T>) + StringUtility.GetRandomString())
        {
            if (!typeof(T).GetTypeInfo().IsInterface)
            {
                throw new NotSupportedException("Not an interface");
            }

            this.eventHubClient = eventHubClient;
            this.Address = address;
            //this.Channel = new AmqpClientProxy(this, typeof(T)).GetChannel();
            this.link = new FaultTolerantAmqpObject<RequestResponseAmqpLink>(t => this.OpenLinkAsync(t), rrlink => rrlink.CloseAsync(TimeSpan.FromSeconds(10)));
            this.clientLinkManager = new ActiveClientLinkManager(this.eventHubClient);
        }

        public string Address { get; }

        public T Channel { get; }

        public override Task CloseAsync()
        {
            return this.link.CloseAsync();
        }

        internal void OnAbort()
        {
            RequestResponseAmqpLink innerLink;
            if (this.link.TryGetOpenedObject(out innerLink))
            {
                innerLink?.Abort();
            }
        }

        async Task<RequestResponseAmqpLink> OpenLinkAsync(TimeSpan timeout)
        {
            ActiveClientRequestResponseLink activeClientLink = await this.eventHubClient.OpenRequestResponseLinkAsync(
                "svc", this.Address, null, AmqpServiceClient<T>.RequiredClaims, timeout);
            this.clientLinkManager.SetActiveLink(activeClientLink);
            return activeClientLink.Link;
        }

        async Task<object> RequestAsync(MethodData md, MethodCallMessage mcm, int argCount)
        {
            RequestResponseAmqpLink requestLink = await this.link.GetOrCreateAsync(TimeSpan.FromMinutes(1));

            ApplicationProperties properties = new ApplicationProperties();
            properties.Map[AmqpClientConstants.ManagementOperationKey] = this.md.Operation.Name;
            // generate message sections containing the arguments
            // convert custom-type parameters if needed
            object bodyValue = null;
            AmqpMap bodyMap = null;
            for (int i = 0; i < argCount; i++)
            {
                ManagementParamAttribute paramAttribute = this.md.Parameters[i];
                object value = SerializationHelper.ToAmqp(this.md.ParameterTypes[i].Serializable, this.mcm.InArgs[i]);

                if (paramAttribute.Location == ManagementParamLocation.ApplicationProperties)
                {
                    properties.Map[paramAttribute.Name] = value;
                }
                else if (paramAttribute.Location == ManagementParamLocation.MapBody)
                {
                    if (bodyMap == null)
                    {
                        bodyMap = new AmqpMap();
                    }

                    bodyMap[new MapKey(paramAttribute.Name)] = value;
                }
                else
                {
                    bodyValue = value;
                }
            }

            // Upsert link RequestProperties to ApplicationProperties 
            foreach (var requestProperty in requestLink.RequestProperties)
            {
                properties.Map[requestProperty.Key] = requestProperty.Value;
            }

            var request = AmqpMessage.Create(new AmqpValue { Value = bodyMap ?? bodyValue });
            request.ApplicationProperties = properties;

            var response = await requestLink.RequestAsync(request, TimeSpan.FromMinutes(1));
            int statusCode = (int)response.ApplicationProperties.Map[AmqpClientConstants.ResponseStatusCode];
            string statusDescription = (string)response.ApplicationProperties.Map[AmqpClientConstants.ResponseStatusDescription];
            if (statusCode != (int)AmqpResponseStatusCode.Accepted && statusCode != (int)AmqpResponseStatusCode.OK)
            {
                AmqpSymbol errorCondition = AmqpExceptionHelper.GetResponseErrorCondition(response, (AmqpResponseStatusCode)statusCode);
                Error error = new Error { Condition = errorCondition, Description = statusDescription };
                throw AmqpExceptionHelper.ToMessagingContract(error);
            }

            object returnValue = null;
            if (response.ValueBody != null)
            {
                returnValue = response.ValueBody.Value;
            }

            if (md.ReturnType.HasValue && returnValue != null)
            {
                Type expected = md.ReturnType.Type;
                SerializableType serializable = md.ReturnType.Serializable;
                if (serializable == null)
                {
                    // must be a generic parameter
                    expected = mcm.GenericTypes[md.ReturnType.Type.GenericParameterPosition];
                    serializable = expected.GetSerializable();
                }

                returnValue = SerializationHelper.FromAmqp(serializable, returnValue);
                if (!expected.IsAssignableFrom(returnValue.GetType()))
                {
                    throw new InvalidOperationException($"Return type mismatch in {mcm.MethodBase.Name}. Expect {expected.Name} Actual {returnValue.GetType().Name}");
                }
            }

            return returnValue;
        }
    }
}
