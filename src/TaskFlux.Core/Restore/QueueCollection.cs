using System.Collections;
using System.Diagnostics;
using TaskFlux.Core.Queue;
using TaskFlux.Domain;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Core.Restore;

/// <summary>
/// Объект, представляющий набор очередей вместе с их данными
/// </summary>
public class QueueCollection
{
    private readonly Dictionary<QueueName, QueueInfo> _queues = new(QueueNameEqualityComparer.Instance);

    /// <summary>
    /// Пуста ли коллекция очередей
    /// </summary>
    public bool IsEmpty => _queues.Count == 0;

    public int Count => _queues.Count;

    public void CreateQueue(QueueName name,
        PriorityQueueCode implementation,
        int? maxQueueSize,
        int? maxMessageSize,
        (long, long)? priorityRange)
    {
        var info = new QueueInfo(name, implementation,
            RecordId.Start /* Очередь создается новая, поэтому начинаем с самого начала */, maxQueueSize,
            maxMessageSize, priorityRange);
        AddQueueInfoCore(info);
    }

    public void DeleteQueue(QueueName name)
    {
        _queues.Remove(name);
    }

    private void AddQueueInfoCore(QueueInfo info)
    {
        Debug.Assert(info is not null, "info is not null");

        try
        {
            _queues.Add(info.QueueName, info);
        }
        catch (ArgumentException e)
        {
            throw new ArgumentException($"Уже была создана очередь с названием {info.QueueName}", e);
        }
    }

    public void AddExistingQueue(QueueName name,
        PriorityQueueCode code,
        RecordId lastRecordId,
        int? maxQueueSize,
        int? maxMessageSize,
        (long, long)? priorityRange,
        IReadOnlyCollection<QueueRecord> records)
    {
        var info = new QueueInfo(name, code, lastRecordId, maxQueueSize, maxMessageSize, priorityRange, records);
        AddQueueInfoCore(info);
    }

    private QueueInfo GetRequiredQueueInfo(QueueName name)
    {
        if (_queues.TryGetValue(name, out var queueInfo))
        {
            return queueInfo;
        }

        throw new ArgumentException($"Очередь с названием {name} не найдена");
    }

    public void AddRecord(QueueName name, long priority, byte[] payload)
    {
        var queue = GetRequiredQueueInfo(name);
        queue.Add(priority, payload);
    }

    public void RemoveRecord(QueueName name, RecordId id)
    {
        var queue = GetRequiredQueueInfo(name);
        queue.Remove(id);
    }

    public static QueueCollection CreateDefault()
    {
        return new QueueCollection() { _queues = { { Defaults.QueueName, Defaults.CreateDefaultQueueInfo() } } };
    }

    public IReadOnlyCollection<QueueInfo> GetAllQueues()
    {
        return new RawQueuesCollection(this);
    }

    private class RawQueuesCollection : IReadOnlyCollection<QueueInfo>
    {
        private readonly QueueCollection _collection;

        public RawQueuesCollection(QueueCollection collection)
        {
            _collection = collection;
        }

        public IEnumerator<QueueInfo> GetEnumerator()
        {
            return _collection._queues.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count => _collection._queues.Count;
    }
}