using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Health;
using System.Fabric.Query;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs.ServiceFabricProcessor;

namespace Microsoft.Azure.EventHubs.Tests.ServiceFabricProcessor
{
    class MockPartitionLister : IFabricPartitionLister
    {
        private readonly int count;
        private readonly int ordinal;

        public MockPartitionLister(int count, int ordinal)
        {
            this.count = count;
            this.ordinal = ordinal;
        }

        public Task<int> GetServiceFabricPartitionCount(Uri serviceFabricServiceName)
        {
            return Task.FromResult<int>(this.count);
        }

        public Task<int> GetServiceFabricPartitionOrdinal(Guid serviceFabricPartitionId)
        {
            return Task.FromResult<int>(this.ordinal);
        }
    }
}
