using Consensus.Core;
using TaskFlux.Core.Queue;

namespace TaskFlux.Serialization;

public class QueueArraySnapshot : ISnapshot
{
    private readonly IReadOnlyCollection<IReadOnlyTaskQueue> _queues;

    public QueueArraySnapshot(IReadOnlyCollection<IReadOnlyTaskQueue> queues)
    {
        _queues = queues;
    }

    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
    {
        return QueuesSnapshotSerializer.Serialize(_queues);
    }
}