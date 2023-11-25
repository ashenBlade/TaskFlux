using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Models;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Serialization.Tests;

public class StubTaskQueue : ITaskQueue
{
    private readonly int? _maxSize;
    private readonly (long, long)? _priority;
    private readonly int? _maxPayloadSize;
    private readonly (long, byte[])[] _data;
    public PriorityQueueCode Code { get; }
    public QueueName Name { get; }
    public int Count => _data.Length;
    public ITaskQueueMetadata Metadata { get; }

    public StubTaskQueue(QueueName name,
                         PriorityQueueCode code,
                         int? maxSize = null,
                         (long, long)? priority = null,
                         int? maxPayloadSize = null,
                         IEnumerable<(long, byte[])>? elements = null)
    {
        _maxSize = maxSize;
        _priority = priority;
        _maxPayloadSize = maxPayloadSize;
        _data = elements?.ToArray()
             ?? Array.Empty<(long, byte[])>();

        Name = name;
        Code = code;
        Metadata = new StubMetadata(this);
    }


    public Result Enqueue(long key, byte[] payload)
    {
        Assert.True(false, "Метод не должен быть вызван при серилазации");
        throw new InvalidOperationException("Нельзя этот методы вызывать во время сериализации");
    }

    public bool TryDequeue(out long key, out byte[] payload)
    {
        Assert.True(false, "Метод не должен быть вызван при серилазации");
        key = default;
        payload = default!;
        return false;
    }

    public IReadOnlyCollection<(long Priority, byte[] Payload)> ReadAllData()
    {
        return _data;
    }

    private class StubMetadata : ITaskQueueMetadata
    {
        private readonly StubTaskQueue _parent;

        public StubMetadata(StubTaskQueue parent)
        {
            _parent = parent;
        }

        public QueueName QueueName => _parent.Name;
        public PriorityQueueCode Code => _parent.Code;
        public int Count => _parent.Count;
        public int? MaxSize => _parent._maxSize;
        public int? MaxPayloadSize => _parent._maxPayloadSize;
        public (long Min, long Max)? PriorityRange => _parent._priority;
    }
}