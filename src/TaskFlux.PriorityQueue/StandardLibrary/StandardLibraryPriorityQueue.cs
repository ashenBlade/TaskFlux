using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("TaskFlux.PriorityQueue.Tests")]

namespace TaskFlux.PriorityQueue.StandardLibrary;

public class StandardLibraryPriorityQueue : IPriorityQueue
{
    private readonly PriorityQueue<byte[], long> _queue = new();
    public int Count => _queue.Count;

    public void Enqueue(long key, byte[] payload)
    {
        _queue.Enqueue(payload, key);
    }

    public bool TryDequeue(out long key, out byte[] value)
    {
        return _queue.TryDequeue(out value!, out key!);
    }

    public IReadOnlyCollection<(long Priority, byte[] Payload)> ReadAllData()
    {
        return new ChunkedQueueDataCollection<long, byte[]>(_queue.UnorderedItems);
    }

    // Для тестов
    internal List<(long, byte[])> GetElementsFromQueue()
    {
        return _queue.UnorderedItems.Select(tuple => ( tuple.Priority, tuple.Element )).ToList();
    }
}