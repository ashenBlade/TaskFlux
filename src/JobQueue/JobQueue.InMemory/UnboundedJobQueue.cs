using JobQueue.Core;
using JobQueue.PriorityQueue;

namespace JobQueue.InMemory;

public class UnboundedJobQueue: IJobQueue
{
    private readonly IPriorityQueue<int, byte[]> _queue;

    public UnboundedJobQueue(IPriorityQueue<int, byte[]> queue)
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