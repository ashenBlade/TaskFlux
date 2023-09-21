using JobQueue.Core;

namespace JobQueue.Serialization.Tests;

public class StubJobQueue : IJobQueue
{
    private readonly uint _maxSize;

    public StubJobQueue(QueueName name, uint maxSize = 0, (long, byte[])[]? elements = null)
    {
        _maxSize = maxSize;
        Name = name;
        _data = elements ?? Array.Empty<(long, byte[])>();
        Metadata = new StubMetadata(this);
    }

    public StubJobQueue(QueueName name, uint maxSize = 0, IEnumerable<(long, byte[])>? elements = null)
    {
        _maxSize = maxSize;
        Name = name;
        _data = elements is null
                    ? Array.Empty<(long, byte[])>()
                    : elements.ToArray();
        Metadata = new StubMetadata(this);
    }

    private readonly (long, byte[])[] _data;

    public QueueName Name { get; }
    public uint Count => ( uint ) _data.Length;
    public IJobQueueMetadata Metadata { get; }

    public bool TryEnqueue(long key, byte[] payload)
    {
        Assert.True(false, "Метод не должен быть вызван при серилазации");
        return false;
    }

    public bool TryDequeue(out long key, out byte[] payload)
    {
        Assert.True(false, "Метод не должен быть вызван при серилазации");
        key = default;
        payload = default!;
        return false;
    }

    public IReadOnlyCollection<(long Priority, byte[] Payload)> GetAllData()
    {
        return _data;
    }

    private class StubMetadata : IJobQueueMetadata
    {
        private readonly StubJobQueue _parent;

        public StubMetadata(StubJobQueue parent)
        {
            _parent = parent;
        }

        public QueueName QueueName => _parent.Name;
        public uint Count => _parent.Count;
        public uint MaxSize => _parent._maxSize;
    }
}