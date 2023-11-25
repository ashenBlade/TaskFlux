using Consensus.Core;
using TaskFlux.Core;
using TaskFlux.Serialization;

namespace Consensus.Application.TaskFlux;

public class ApplicationSnapshot : ISnapshot
{
    private readonly IApplication _application;

    public ApplicationSnapshot(IApplication application)
    {
        _application = application;
    }

    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
    {
        return QueuesSnapshotSerializer.Serialize(_application.TaskQueueManager.GetAllQueues());
    }
}