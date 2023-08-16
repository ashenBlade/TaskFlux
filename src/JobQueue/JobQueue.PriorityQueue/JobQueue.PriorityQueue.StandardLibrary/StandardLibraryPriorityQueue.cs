using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

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

    // Для тестов
    internal List<(TKey, TValue)> GetElementsFromQueue()
    {
        return _queue.UnorderedItems.Select(tuple => ( tuple.Priority, tuple.Element )).ToList();
    }
}