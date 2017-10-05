using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    class ActorThrottle
    {
        private Task[] pendingCalls;

        internal ActorThrottle(int maxCalls)
        {
            this.pendingCalls = new Task[maxCalls];
            for (int i = 0; i < maxCalls; i++)
            {
                this.pendingCalls[i] = Task.CompletedTask;
            }
        }

        internal int GetAvailableSlot()
        {
            return Task.WaitAny(this.pendingCalls);
        }

        internal void AddPendingCall(Task pending, int slot)
        {
            this.pendingCalls[slot] = pending;
        }

        internal void WaitForFinish()
        {
            Task.WaitAll(this.pendingCalls, Constants.FinishUserCallsOnCloseTimeout);
        }
    }
}
