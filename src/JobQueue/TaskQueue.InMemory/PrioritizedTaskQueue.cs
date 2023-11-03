using TaskQueue.Core;
using TaskQueue.PriorityQueue;

namespace TaskQueue.InMemory;

/// <summary>
/// Реализация очереди задач, использующая ключи в качестве приоритета
/// </summary>
public class PrioritizedTaskQueue : ITaskQueue
{
    public QueueName Name { get; }
    public ITaskQueueMetadata Metadata { get; }
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

    public PrioritizedTaskQueue(QueueName name, uint maxCount, IPriorityQueue<long, byte[]> queue)
    {
        Name = name;
        _queue = queue;
        _max = maxCount;
        Metadata = new PrioritizedTaskQueueMetadata(this);
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

    public static PrioritizedTaskQueue CreateUnbounded(QueueName name, IPriorityQueue<long, byte[]> queue)
    {
        return new PrioritizedTaskQueue(name, 0, queue);
    }

    public static PrioritizedTaskQueue CreateBounded(QueueName name, IPriorityQueue<long, byte[]> queue, uint maxSize)
    {
        return new PrioritizedTaskQueue(name, maxSize, queue);
    }

    private class PrioritizedTaskQueueMetadata : ITaskQueueMetadata
    {
        private readonly PrioritizedTaskQueue _parent;

        public PrioritizedTaskQueueMetadata(PrioritizedTaskQueue parent)
        {
            _parent = parent;
        }

        public QueueName QueueName => _parent.Name;
        public uint Count => ( uint ) _parent.Count;
        public uint MaxSize => _parent._max;
    }
}