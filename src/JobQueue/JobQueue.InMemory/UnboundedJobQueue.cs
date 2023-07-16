using JobQueue.Core;
using JobQueue.SortedQueue;

namespace JobQueue.InMemory;

public class UnboundedJobQueue: IJobQueue
{
    private readonly ISortedQueue<int, byte[]> _queue;

    public UnboundedJobQueue(ISortedQueue<int, byte[]> queue)
    {
        _queue = queue;
    }
    
    public bool TryEnqueue(int key, byte[] payload)
    {
        _queue.Enqueue(key, payload);
        return true;
    }

    public bool TryDequeue(out int key, out byte[] payload)
    {
        return _queue.TryDequeue(out key, out payload);
    }

    public int Count => _queue.Count;
}