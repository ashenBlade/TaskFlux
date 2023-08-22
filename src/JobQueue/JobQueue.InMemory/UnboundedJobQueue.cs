using JobQueue.Core;
using JobQueue.PriorityQueue;

namespace JobQueue.InMemory;

/// <summary>
/// Очередь без ограничений на размер
/// </summary>
public class UnboundedJobQueue : IJobQueue
{
    public QueueName Name { get; }
    public int Count => _queue.Count;
    public IJobQueueMetadata Metadata { get; }
    private readonly IPriorityQueue<long, byte[]> _queue;

    public UnboundedJobQueue(QueueName name, IPriorityQueue<long, byte[]> queue)
    {
        Name = name;
        _queue = queue;
        Metadata = new UnboundedJobQueueMetadata(this);
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

    public IReadOnlyCollection<(long Priority, byte[] Payload)> GetAllData()
    {
        return _queue.ReadAllData();
    }

    private class UnboundedJobQueueMetadata : IJobQueueMetadata
    {
        private readonly UnboundedJobQueue _queue;
        public QueueName QueueName => _queue.Name;
        public uint Count => ( uint ) _queue.Count;
        public uint MaxSize => 0;

        public UnboundedJobQueueMetadata(UnboundedJobQueue queue)
        {
            _queue = queue;
        }
    }
}