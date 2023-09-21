namespace JobQueue.PriorityQueue;

public interface IPriorityQueue<TKey, TValue>
{
    public int Count { get; }
    public void Enqueue(TKey key, TValue value);
    public bool TryDequeue(out TKey key, out TValue value);
    IReadOnlyCollection<(TKey Priority, TValue Payload)> ReadAllData();
}