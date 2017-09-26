using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Actors.Client;
using Microsoft.ServiceFabric.Services.Client;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Description;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    class ServiceUtilities
    {
        internal static bool IsPartitionZero(ServicePartitionInformation partitionInfo)
        {
            return (((Int64RangePartitionInformation)partitionInfo).LowKey == long.MinValue);
        }


        private static Object cachedMapperLock = new object();
        private static IUserActor cachedMapper = null;
        internal static IUserActor Mapper
        {
            get
            {
                lock (ServiceUtilities.cachedMapperLock)
                {
                    if (ServiceUtilities.cachedMapper == null)
                    {
                        ServiceUtilities.cachedMapper = ActorProxy.Create<IUserActor>(Constants.MapperActorId, EventProcessorActorService.ServiceName);
                    }
                }
                return ServiceUtilities.cachedMapper;
            }
        }

        internal static async Task<IUserActor> GetMetricReporterForPartition(ServicePartitionInformation partitionInfo, CancellationToken cancellationToken)
        {
            Int64RangePartitionInformation int64PartitionInfo = (Int64RangePartitionInformation)partitionInfo;
            string stringId = Constants.MetricReporterActorIdPrefix + int64PartitionInfo.Id.ToString();
            ActorId reporterId = await ServiceUtilities.Mapper.GetOrAddActorInPartition(stringId, int64PartitionInfo, cancellationToken);
            return ActorProxy.Create<IUserActor>(reporterId, EventProcessorActorService.ServiceName);
        }

        internal static async Task<IUserActor> GetPersistorForPartition(ServicePartitionInformation partitionInfo, CancellationToken cancellationToken)
        {
            Int64RangePartitionInformation int64PartitionInfo = (Int64RangePartitionInformation)partitionInfo;
            string stringId = Constants.PersistorActorIdPrefix + int64PartitionInfo.Id.ToString();
            ActorId persistorId = await ServiceUtilities.Mapper.GetOrAddActorInPartition(stringId, int64PartitionInfo, cancellationToken);
            return ActorProxy.Create<IUserActor>(persistorId, EventProcessorActorService.ServiceName);
        }

        private static Object cachedServicePartitionMapLock = new object();
        private static Dictionary<int, Int64RangePartitionInformation> cachedServicePartitionMap = null;
        internal static async Task<Dictionary<int, Int64RangePartitionInformation>> GetServicePartitionMap(CancellationToken cancellationToken)
        {
            if (ServiceUtilities.cachedServicePartitionMap == null)
            {
                ServicePartitionResolver resolver = ServicePartitionResolver.GetDefault();
                Int64RangePartitionInformation scanner = null;
                Dictionary<int, Int64RangePartitionInformation> map = new Dictionary<int, Int64RangePartitionInformation>();
                long lowScan = long.MinValue;
                int ordinal = 0;
                do
                {
                    ServicePartitionKey resolveKey = new ServicePartitionKey(lowScan);
                    ResolvedServicePartition partition = await resolver.ResolveAsync(EventProcessorActorService.ServiceName, resolveKey, cancellationToken);
                    scanner = (Int64RangePartitionInformation)partition.Info;
                    map.Add(ordinal++, scanner);
                    lowScan = scanner.HighKey + 1;
                } while (scanner.HighKey != long.MaxValue);

                lock (ServiceUtilities.cachedServicePartitionMapLock)
                {
                    if (ServiceUtilities.cachedServicePartitionMap == null)
                    {
                        ServiceUtilities.cachedServicePartitionMap = map;
                    }
                }
            }

            return ServiceUtilities.cachedServicePartitionMap;
        }

        internal static async Task<int> GetServicePartitionOrdinal(ServicePartitionInformation thisPartition, CancellationToken cancellationToken)
        {
            await GetServicePartitionMap(cancellationToken);
            int result;
            for (result = 0; result < ServiceUtilities.cachedServicePartitionMap.Count; result++)
            {
                if (ServiceUtilities.cachedServicePartitionMap[result].Equals(thisPartition))
                {
                    break;
                }
            }
            return result < ServiceUtilities.cachedServicePartitionMap.Count ? result : -1;
        }

        internal static async Task<int> GetServicePartitionCount(CancellationToken cancellationToken)
        {
            await GetServicePartitionMap(cancellationToken);
            return ServiceUtilities.cachedServicePartitionMap.Count;
        }

        internal static string GetConfigurationValue(string configurationValueName, string defaultValue = null)
        {
            string value = defaultValue;
            ConfigurationPackage configPackage = EventProcessorActorService.ServiceContext.CodePackageActivationContext.GetConfigurationPackageObject("Config");
            try
            {
                ConfigurationSection configSection = configPackage.Settings.Sections[Constants.EventProcessorConfigSectionName];
                ConfigurationProperty configProp = configSection.Parameters[configurationValueName];
            }
            catch (KeyNotFoundException e)
            {
                // If the user has not specified a value in config, drop through and return the default value.
                // If the caller cannot continue without a value, it is up to the caller to detect and handle.
            }
            // catch ( ArgumentNullException e) if configurationValueName is null, that's a code bug, do not catch
            return value;
        }
    }
}
