using TaskFlux.Consensus;
using TaskFlux.Core.Queue;

namespace TaskFlux.Application.Persistence;

public static class StateRestorer
{
    public static QueueCollection RestoreState(ISnapshot? snapshot, IEnumerable<byte[]> deltas)
    {
        QueueCollection collection;
        if (snapshot is not null)
        {
            // В будущем лучше сделать отдельный тип потока, который читает из снапшота
            // - считывать все в память не очень оптимально
            var memory = new MemoryStream();
            foreach (var chunk in snapshot.GetAllChunks())
            {
                memory.Write(chunk.Span);
            }

            memory.Position = 0;

            collection = QueuesSnapshotSerializer.Deserialize(memory);
        }
        else
        {
            collection = new QueueCollection();
            var defaultQueue = TaskQueueBuilder.CreateDefault();
            var metadata = defaultQueue.Metadata;
            collection.AddExistingQueue(defaultQueue.Name, defaultQueue.Code, metadata.MaxQueueSize,
                metadata.MaxPayloadSize, metadata.PriorityRange, Array.Empty<QueueRecord>());
        }

        foreach (var delta in deltas.Select(Delta.Delta.DeserializeFrom))
        {
            delta.Apply(collection);
        }

        return collection;
    }
}