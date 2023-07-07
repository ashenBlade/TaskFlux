using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("JobQueue.SortedQueue.Tests")]
namespace JobQueue.SortedQueue;

public class PriorityQueueSortedQueue<TKey, TValue> : ISortedQueue<TKey, TValue>
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

    // Для тестов
    internal List<(TKey, TValue)> GetElementsFromQueue()
    {
        return _queue.UnorderedItems.Select(tuple => ( tuple.Priority, tuple.Element )).ToList();
    }
}