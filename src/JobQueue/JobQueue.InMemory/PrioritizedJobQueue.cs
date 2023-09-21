using JobQueue.Core;
using JobQueue.PriorityQueue;

namespace JobQueue.InMemory;

/// <summary>
/// Реализация очереди задач, использующая ключи в качестве приоритета
/// </summary>
public class PrioritizedJobQueue : IJobQueue
{
    public QueueName Name { get; }
    public IJobQueueMetadata Metadata { get; }
    public uint Count => ( uint ) _queue.Count;

    private readonly IPriorityQueue<long, byte[]> _queue;

    /// <summary>
    /// Максимальное количество элементов в очереди.
    /// 0 означает отсутствие предела
    /// </summary>
    private readonly uint _max;

    private const uint UnboundedQueueCount = 0;

    private bool IsLimitReached()
    {
        return _max != UnboundedQueueCount
            && _max <= _queue.Count;
    }

    public PrioritizedJobQueue(QueueName name, uint maxCount, IPriorityQueue<long, byte[]> queue)
    {
        Name = name;
        _queue = queue;
        _max = maxCount;
        Metadata = new PrioritizedJobQueueMetadata(this);
    }

    public bool TryEnqueue(long key, byte[] payload)
    {
        if (IsLimitReached())
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

    public IReadOnlyCollection<(long Priority, byte[] Payload)> GetAllData()
    {
        return _queue.ReadAllData();
    }

    public static PrioritizedJobQueue CreateUnbounded(QueueName name, IPriorityQueue<long, byte[]> queue)
    {
        return new PrioritizedJobQueue(name, 0, queue);
    }

    public static PrioritizedJobQueue CreateBounded(QueueName name, IPriorityQueue<long, byte[]> queue, uint maxSize)
    {
        return new PrioritizedJobQueue(name, maxSize, queue);
    }

    private class PrioritizedJobQueueMetadata : IJobQueueMetadata
    {
        private readonly PrioritizedJobQueue _parent;

        public PrioritizedJobQueueMetadata(PrioritizedJobQueue parent)
        {
            _parent = parent;
        }

        public QueueName QueueName => _parent.Name;
        public uint Count => ( uint ) _parent.Count;
        public uint MaxSize => _parent._max;
    }
}