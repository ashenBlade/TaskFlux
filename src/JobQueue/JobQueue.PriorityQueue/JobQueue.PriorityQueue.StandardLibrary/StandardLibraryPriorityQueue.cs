using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("JobQueue.PriorityQueue.StandardLibrary.Tests")]

namespace JobQueue.PriorityQueue.StandardLibrary;

public class StandardLibraryPriorityQueue<TKey, TValue> : IPriorityQueue<TKey, TValue>
{
    private readonly PriorityQueue<TValue, TKey> _queue = new();
    public int Count => _queue.Count;

    public void Enqueue(TKey key, TValue value)
    {
        _queue.Enqueue(value, key);
    }

    public bool TryDequeue(out TKey key, out TValue value)
    {
        return _queue.TryDequeue(out value!, out key!);
    }

    public IReadOnlyCollection<(TKey Priority, TValue Payload)> ReadAllData()
    {
        return new ChunkedQueueDataCollection<TKey, TValue>(_queue.UnorderedItems);
    }

    // Для тестов
    internal List<(TKey, TValue)> GetElementsFromQueue()
    {
        return _queue.UnorderedItems.Select(tuple => ( tuple.Priority, tuple.Element )).ToList();
    }
}