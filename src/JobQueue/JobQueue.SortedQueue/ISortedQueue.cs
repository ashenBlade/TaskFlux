namespace JobQueue.SortedQueue;

public interface ISortedQueue<TKey, TValue>
{
    public int Count { get; }
    public void Enqueue(TKey key, TValue value);
    public bool TryDequeue(out TKey key, out TValue value);
}