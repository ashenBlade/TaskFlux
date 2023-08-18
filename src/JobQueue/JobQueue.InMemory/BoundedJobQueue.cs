using System.Diagnostics;
using System.Threading.Channels;
using JobQueue.Core;
using JobQueue.PriorityQueue;

namespace JobQueue.InMemory;

/// <summary>
/// Очередь с ограничением на размер
/// </summary>
public class BoundedJobQueue: IJobQueue
{
    public IJobQueueMetadata Metadata { get; }
    private readonly IPriorityQueue<long, byte[]> _queue;
    private readonly uint _limit;

    /// <summary>
    /// Основной конструктор для ограниченной очереди
    /// </summary>
    /// <param name="name">Название очереди</param>
    /// <param name="queue">Хранилище для данных</param>
    /// <param name="limit">Максимальный размер очереди</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="limit"/> - <c>0</c></exception>
    public BoundedJobQueue(QueueName name, IPriorityQueue<long, byte[]> queue, uint limit)
    {
        if (limit == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), limit,
                "Максимальный размер очереди не может быть равен 0. Чтобы убрать лимит надо использовать UnboundedJobQueue");
        }
        
        Name = name;
        _queue = queue;
        _limit = limit;
        Metadata = new BoundedJobQueueMetadata(this);
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

    public QueueName Name { get; }
    public int Count => _queue.Count;

    private class BoundedJobQueueMetadata : IJobQueueMetadata
    {
        private readonly BoundedJobQueue _queue;
        
        public QueueName QueueName => _queue.Name;
        public uint Count => (uint) _queue.Count;
        public uint MaxSize => _queue._limit;

        public BoundedJobQueueMetadata(BoundedJobQueue queue)
        {
            _queue = queue;
        }
    }
}