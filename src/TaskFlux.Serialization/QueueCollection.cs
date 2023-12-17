using System.Collections;
using TaskFlux.Core.Queue;
using TaskFlux.Models;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Serialization;

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
        var info = new QueueInfo(name, implementation, maxQueueSize, maxMessageSize, priorityRange);
        try
        {
            _queues.Add(name, info);
        }
        catch (ArgumentException e)
        {
            throw new ArgumentException($"Уже была создана очередь с названием {name.Name}", e);
        }
    }

    public void DeleteQueue(QueueName name)
    {
        _queues.Remove(name);
    }

    internal void AddExistingQueue(QueueName name,
                                   PriorityQueueCode code,
                                   int? maxQueueSize,
                                   int? maxMessageSize,
                                   (long, long)? priorityRange,
                                   IReadOnlyCollection<QueueRecord> data)
    {
        var info = new QueueInfo(name, code, maxQueueSize, maxMessageSize, priorityRange);
        if (data.Count > 0)
        {
            foreach (var record in data)
            {
                info.Add(record.Key, record.Message);
            }
        }

        try
        {
            _queues.Add(name, info);
        }
        catch (ArgumentException e)
        {
            throw new ArgumentException($"Очередь с названием {name.Name} уже присутствует", e);
        }
    }

    public void AddRecord(QueueName name, long priority, byte[] data)
    {
        if (_queues.TryGetValue(name, out var info))
        {
            info.Add(priority, data);
        }
        else
        {
            throw new ArgumentException($"Очередь с названием {name} не найдена");
        }
    }

    public void RemoveRecord(QueueName name, long priority, byte[] data)
    {
        if (_queues.TryGetValue(name, out var info))
        {
            info.Remove(priority, data);
        }
        else
        {
            throw new ArgumentException($"Очередь с названием {name} не найдена");
        }
    }

    public IReadOnlyCollection<(QueueName Name, PriorityQueueCode Code, int? MaxQueueSize, int? MaxPayloadSize, (long,
        long)? PriorityRange, IReadOnlyCollection<(long Key, byte[] Payload)> Data)> GetQueuesRaw()
    {
        return new RawQueuesCollection(this);
    }

    public IReadOnlyCollection<ITaskQueue> BuildQueues()
    {
        var result = new List<ITaskQueue>(_queues.Count);
        result.AddRange(_queues.Select(q => q.Value.Build()));
        return result;
    }

    private class RawQueuesCollection : IReadOnlyCollection<(QueueName Name, PriorityQueueCode Code, int? MaxQueueSize,
        int? MaxPayloadSize, (long, long)? PriorityRange, IReadOnlyCollection<(long Key, byte[] Payload)> Data)>
    {
        private readonly QueueCollection _collection;

        public RawQueuesCollection(QueueCollection collection)
        {
            _collection = collection;
        }

        public IEnumerator<(QueueName Name, PriorityQueueCode Code, int? MaxQueueSize, int? MaxPayloadSize, (long, long)
            ? PriorityRange, IReadOnlyCollection<(long Key, byte[] Payload)> Data)> GetEnumerator()
        {
            return _collection._queues
                              .Select(x => x.Value.ToTuple())
                              .GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count => _collection._queues.Count;
    }
}