using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Core.Restore;
using TaskFlux.Core.Subscription;
using TaskFlux.PriorityQueue;
using Xunit;

namespace TaskFlux.Persistence.Tests;

public class StubTaskQueue : ITaskQueue
{
    private readonly int? _maxSize;
    private readonly (long, long)? _priority;
    private readonly int? _maxPayloadSize;
    private readonly QueueRecord[] _data;
    public PriorityQueueCode Code { get; }
    public QueueName Name { get; }
    public int Count => _data.Length;
    public ITaskQueueMetadata Metadata { get; }

    public StubTaskQueue(QueueName name,
                         PriorityQueueCode code,
                         int? maxSize = null,
                         (long, long)? priority = null,
                         int? maxPayloadSize = null,
                         IEnumerable<QueueRecord>? elements = null)
    {
        _maxSize = maxSize;
        _priority = priority;
        _maxPayloadSize = maxPayloadSize;
        _data = elements?.ToArray() ?? Array.Empty<QueueRecord>();

        Name = name;
        Code = code;
        Metadata = new StubMetadata(this);
    }


    public EnqueueResult Enqueue(long priority, byte[] payload)
    {
        Assert.True(false, "Метод не должен быть вызван при серилазации");
        throw new InvalidOperationException("Нельзя этот методы вызывать во время сериализации");
    }

    public bool TryDequeue(out QueueRecord record)
    {
        Assert.True(false, "Метод не должен быть вызван при серилазации");
        record = default!;
        return false;
    }

    public IQueueSubscriber Subscribe()
    {
        return new StubQueueSubscriber();
    }

    public IReadOnlyCollection<QueueRecord> ReadAllData()
    {
        return _data;
    }

    private class StubQueueSubscriber : IQueueSubscriber
    {
        public async ValueTask<QueueRecord> WaitRecordAsync(CancellationToken token)
        {
            throw new Exception("Метод не должен быть вызван на стабе очереди");
        }

        public void Dispose()
        {
        }
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
        public int? MaxQueueSize => _parent._maxSize;
        public int? MaxPayloadSize => _parent._maxPayloadSize;
        public (long Min, long Max)? PriorityRange => _parent._priority;
    }

    public static StubTaskQueue FromQueueInfo(QueueInfo info)
    {
        return new StubTaskQueue(info.QueueName, info.Code, info.MaxQueueSize, info.PriorityRange, info.MaxPayloadSize,
            info.Data);
    }
}