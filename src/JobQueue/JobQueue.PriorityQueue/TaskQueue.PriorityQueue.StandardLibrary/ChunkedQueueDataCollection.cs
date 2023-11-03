using System.Collections;

namespace TaskQueue.PriorityQueue.StandardLibrary;

public class ChunkedQueueDataCollection<TKey, TValue> : IReadOnlyCollection<(TKey, TValue)>
{
    private readonly PriorityQueue<TValue, TKey>.UnorderedItemsCollection _collection;

    public ChunkedQueueDataCollection(PriorityQueue<TValue, TKey>.UnorderedItemsCollection collection)
    {
        _collection = collection;
    }

    public IEnumerator<(TKey, TValue)> GetEnumerator()
    {
        foreach (var (data, priority) in _collection)
        {
            yield return ( priority, data );
        }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public int Count => _collection.Count;
}