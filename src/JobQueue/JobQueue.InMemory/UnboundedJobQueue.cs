using JobQueue.Core;
using JobQueue.PriorityQueue;

namespace JobQueue.InMemory;

/// <summary>
/// Очередь без ограничений на размер
/// </summary>
public class UnboundedJobQueue: IJobQueue
{
    private readonly IPriorityQueue<long, byte[]> _queue;

    public UnboundedJobQueue(IPriorityQueue<long, byte[]> queue)
    {
        _queue = queue;
    }
    
    public bool TryEnqueue(long key, byte[] payload)
    {
        _queue.Enqueue(key, payload);
        return true;
    }

    public bool TryDequeue(out long key, out byte[] payload)
    {
        return _queue.TryDequeue(out key, out payload);
    }

    public int Count => _queue.Count;
}