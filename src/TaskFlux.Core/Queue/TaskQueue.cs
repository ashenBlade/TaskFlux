using System.Collections;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Waiter;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Core.Queue;

internal class TaskQueue : ITaskQueue
{
    public PriorityQueueCode Code => _queue.Code;
    public QueueName Name { get; }
    private readonly QueuePolicy[] _policies;
    private readonly IPriorityQueue _queue;
    public int Count => _queue.Count;
    private IQueueSubscriberManager _awaiterManager;

    public TaskQueue(QueueName name,
                     IPriorityQueue queue,
                     QueuePolicy[] policies,
                     IQueueSubscriberManager awaiterManager)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(policies);

        _policies = policies;
        _awaiterManager = awaiterManager;
        _queue = queue;
        Name = name;
    }

    public EnqueueResult Enqueue(long priority, byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        foreach (var policy in _policies)
        {
            if (!policy.CanEnqueue(priority, payload, this))
            {
                return EnqueueResult.PolicyViolation(policy);
            }
        }

        // Вставку в очередь необходимо сделать, только в случае если никто новых записей не ожидает
        if (!_awaiterManager.TryNotifyRecord(new QueueRecord(priority, payload)))
        {
            _queue.Enqueue(priority, payload);
        }

        return EnqueueResult.Success();
    }

    public bool TryDequeue(out QueueRecord record)
    {
        if (_queue.TryDequeue(out var priority, out var payload))
        {
            record = new QueueRecord(priority, payload);
            return true;
        }

        record = default;
        return false;
    }

    public IQueueSubscriber GetRecordAwaiter()
    {
        // О новых записях не уведомляем, т.к. этот метод должен быть вызван только когда очередь была пуста при вызове TryDequeue
        return _awaiterManager.GetSubscriber();
    }

    public IReadOnlyCollection<QueueRecord> ReadAllData()
    {
        return new PriorityQueueRecordCollection(_queue.ReadAllData());
    }

    private class PriorityQueueRecordCollection : IReadOnlyCollection<QueueRecord>
    {
        private readonly IReadOnlyCollection<(long Key, byte[] Payload)> _queueCollection;

        public PriorityQueueRecordCollection(IReadOnlyCollection<(long Key, byte[] Payload)> queueCollection)
        {
            _queueCollection = queueCollection;
        }

        public IEnumerator<QueueRecord> GetEnumerator()
        {
            foreach (var (priority, payload) in _queueCollection)
            {
                yield return new QueueRecord(priority, payload);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count => _queueCollection.Count;
    }

    private TaskQueueMetadata? _metadata;
    public ITaskQueueMetadata Metadata => _metadata ??= CreateMetadata();

    private TaskQueueMetadata CreateMetadata()
    {
        var metadata = new TaskQueueMetadata(this);
        foreach (var policy in _policies)
        {
            policy.Enrich(metadata);
        }

        return metadata;
    }
}