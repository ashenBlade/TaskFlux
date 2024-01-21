using TaskFlux.Consensus;
using TaskFlux.Core;

namespace TaskFlux.Application.Persistence;

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