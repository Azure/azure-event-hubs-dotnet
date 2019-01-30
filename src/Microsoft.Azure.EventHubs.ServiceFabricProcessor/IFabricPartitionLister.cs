using System;
using System.Fabric.Query;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    /// <summary>
    /// 
    /// </summary>
    public interface IFabricPartitionLister
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="serviceFabricServiceName"></param>
        /// <returns></returns>
        Task<int> GetServiceFabricPartitionCount(Uri serviceFabricServiceName);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="serviceFabricPartitionId"></param>
        /// <returns></returns>
        Task<int> GetServiceFabricPartitionOrdinal(Guid serviceFabricPartitionId);
    }
}
