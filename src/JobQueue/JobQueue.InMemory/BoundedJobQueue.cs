using System.Diagnostics;
using JobQueue.Core;
using JobQueue.PriorityQueue;

namespace JobQueue.InMemory;

public class BoundedJobQueue: IJobQueue
{
    private readonly IPriorityQueue<long, byte[]> _queue;
    private readonly int _limit;

    public BoundedJobQueue(IPriorityQueue<long, byte[]> queue, int limit)
    {
        _queue = queue;
        _limit = limit;
    }

    public bool TryEnqueue(long key, byte[] payload)
    {
        Debug.Assert(payload != null);
        if (_queue.Count <= _limit)
        {
            return false;
        }
        
        _queue.Enqueue(key, payload);
        return true;

    }

    public bool TryDequeue(out long key, out byte[] payload)
    {
        return _queue.TryDequeue(out key, out payload);
    }

    public int Count => _queue.Count;
}