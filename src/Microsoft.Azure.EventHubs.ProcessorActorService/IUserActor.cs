using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.EventHubs.ProcessorActorService
{
    public interface IUserActor : IEventProcessorActor
    {
        Task OnReceive(IEnumerable<EventData> events, CancellationToken cancellationToken);
    }
}
