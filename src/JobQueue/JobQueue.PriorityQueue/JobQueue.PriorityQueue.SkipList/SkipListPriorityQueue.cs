namespace JobQueue.PriorityQueue.SkipList;

public class SkipListPriorityQueue<TKey, TValue>: IPriorityQueue<TKey, TValue>
{
    public int Count { get; }
    public void Enqueue(TKey key, TValue value)
    {
        throw new NotImplementedException();
    }

    public bool TryDequeue(out TKey key, out TValue value)
    {
        throw new NotImplementedException();
    }
}