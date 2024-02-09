using TaskFlux.Consensus;

namespace TaskFlux.Persistence.ApplicationState;

public class QueueCollectionSnapshot : ISnapshot
{
    private readonly QueueCollection _collection;

    public QueueCollectionSnapshot(QueueCollection collection)
    {
        _collection = collection;
    }

    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
    {
        return QueuesSnapshotSerializer.Serialize(_collection);
    }
}