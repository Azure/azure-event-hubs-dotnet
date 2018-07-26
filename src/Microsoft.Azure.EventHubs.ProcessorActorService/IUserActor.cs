using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    /// <summary>
    /// 
    /// </summary>
    public interface IUserActor : IEventProcessorActor
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="events"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task OnReceive(IEnumerable<EventData> events, CancellationToken cancellationToken);
    }
}
