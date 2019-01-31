using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;

namespace Microsoft.Azure.EventHubs.Tests.ServiceFabricProcessor
{
    class MockTransaction : ITransaction
    {
        private static long sequence = -1L;

        public MockTransaction()
        {
            this.CommitSequenceNumber = Interlocked.Increment(ref MockTransaction.sequence);
            this.TransactionId = this.CommitSequenceNumber;
        }

        public long CommitSequenceNumber { private set; get; }

        public long TransactionId { private set; get; }

        public void Abort()
        {
            // nothing to do
        }

        public Task CommitAsync()
        {
            // nothing to do
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            // nothing to do
        }

        public Task<long> GetVisibilitySequenceNumberAsync()
        {
            return Task.FromResult<long>(this.CommitSequenceNumber);
        }
    }
}
