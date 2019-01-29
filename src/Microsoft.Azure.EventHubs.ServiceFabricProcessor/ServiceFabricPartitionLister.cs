using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    class ServiceFabricPartitionLister : IFabricPartitionLister
    {
        private ServicePartitionList partitionList = null;

        public async Task<int> GetServiceFabricPartitionCount(Uri serviceFabricServiceName)
        {
            using (FabricClient fabricClient = new FabricClient())
            {
                this.partitionList = await fabricClient.QueryManager.GetPartitionListAsync(serviceFabricServiceName);
            }
            return this.partitionList.Count;
        }

        public Task<int> GetServiceFabricPartitionOrdinal(Guid serviceFabricPartitionId)
        {
            int ordinal = -1;
            for (int a = 0; a < partitionList.Count; a++)
            {
                if (this.partitionList[a].PartitionInformation.Id == serviceFabricPartitionId)
                {
                    ordinal = a;
                    break;
                }
            }
            return ordinal;
        }
    }
}
