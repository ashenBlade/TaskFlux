using TaskQueue.Core;

namespace TaskQueue.Serialization.Tests;

public class StubTaskQueue : ITaskQueue
{
    private readonly int? _maxSize;
    private readonly (long, long)? _priority;
    private readonly uint? _maxPayloadSize;

    public StubTaskQueue(QueueName name,
                         int? maxSize = null,
                         (long, long)? priority = null,
                         uint? maxPayloadSize = null,
                         (long, byte[])[]? elements = null)
    {
        _maxSize = maxSize;
        _priority = priority;
        _maxPayloadSize = maxPayloadSize;
        Name = name;
        _data = elements ?? Array.Empty<(long, byte[])>();
        Metadata = new StubMetadata(this);
    }

    public StubTaskQueue(QueueName name,
                         int? maxSize = null,
                         (long, long)? priority = null,
                         uint? maxPayloadSize = null,
                         IEnumerable<(long, byte[])>? elements = null)
    {
        _maxSize = maxSize;
        _priority = priority;
        _maxPayloadSize = maxPayloadSize;
        Name = name;
        _data = elements is null
                    ? Array.Empty<(long, byte[])>()
                    : elements.ToArray();
        Metadata = new StubMetadata(this);
    }

    private readonly (long, byte[])[] _data;

    public QueueName Name { get; }
    public int Count => _data.Length;
    public ITaskQueueMetadata Metadata { get; }

    public EnqueueResult Enqueue(long key, byte[] payload)
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
        public int Count => _parent.Count;
        public int? MaxSize => _parent._maxSize;
        public uint? MaxPayloadSize => _parent._maxPayloadSize;
        public (long Min, long Max)? PriorityRange => _parent._priority;
    }
}