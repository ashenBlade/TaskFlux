using System.Collections;
using System.Diagnostics;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Subscription;
using TaskFlux.Domain;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Core.Queue;

internal class TaskQueue : ITaskQueue
{
    public PriorityQueueCode Code => _queue.Code;
    public QueueName Name { get; }

    private readonly QueuePolicy[] _policies;
    private readonly IPriorityQueue<PriorityQueueData> _queue;
    private readonly IQueueSubscriberManager _awaiterManager;
    public int Count => _queue.Count;
    public IReadOnlyList<QueuePolicy> Policies => _policies;

    public RecordId LastId { get; private set; }

    public TaskQueue(RecordId lastId,
        QueueName name,
        IPriorityQueue<PriorityQueueData> queue,
        QueuePolicy[] policies,
        IQueueSubscriberManager awaiterManager)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(policies);
        ArgumentNullException.ThrowIfNull(awaiterManager);

        _policies = policies;
        _awaiterManager = awaiterManager;
        _queue = queue;
        Name = name;
        LastId = lastId;
    }

    /// <summary>
    /// Получить новый ID записи и обновить свой последний Id
    /// </summary>
    /// <returns>ID для новой записи</returns>
    private RecordId ObtainNewRecordId() => LastId = LastId.Increment();

    public QueueRecord Enqueue(long priority, byte[] payload)
    {
        Debug.Assert(payload is not null, "payload is not null");
        ArgumentNullException.ThrowIfNull(payload);

        var id = ObtainNewRecordId();
        var record = new QueueRecord(id, priority, payload);

        // Вставку в очередь необходимо сделать, только в случае если никто новых записей не ожидает
        if (!_awaiterManager.TryNotifyRecord(record))
        {
            _queue.Enqueue(priority, record.GetPriorityQueueData());
        }

        return record;
    }

    public void EnqueueExisting(QueueRecord record)
    {
        if (!_awaiterManager.TryNotifyRecord(record))
        {
            _queue.Enqueue(record.Priority, record.GetPriorityQueueData());
        }
    }

    public bool TryDequeue(out QueueRecord record)
    {
        if (_queue.TryDequeue(out var priority, out var data))
        {
            record = new QueueRecord(data.Id, priority, data.Payload);
            return true;
        }

        record = default;
        return false;
    }

    public IQueueSubscriber Subscribe()
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
        private readonly IReadOnlyCollection<(long Key, PriorityQueueData IdPayload)> _queueCollection;

        public PriorityQueueRecordCollection(
            IReadOnlyCollection<(long Key, PriorityQueueData IdPayload)> queueCollection)
        {
            _queueCollection = queueCollection;
        }

        public IEnumerator<QueueRecord> GetEnumerator()
        {
            foreach (var (priority, data) in _queueCollection)
            {
                yield return new QueueRecord(data.Id, priority, data.Payload);
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