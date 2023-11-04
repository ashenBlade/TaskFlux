using TaskQueue.Core.Policies;
using TaskQueue.PriorityQueue;

namespace TaskQueue.Core;

internal class TaskQueue : ITaskQueue
{
    public QueueName Name { get; }
    public int Count => _queue.Count;

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

    public TaskQueue(QueueName name, IPriorityQueue<long, byte[]> queue, QueuePolicy[] policies)
    {
        _policies = policies;
        _queue = queue;
        Name = name;
    }

    private readonly QueuePolicy[] _policies;
    private readonly IPriorityQueue<long, byte[]> _queue;

    public IReadOnlyCollection<(long Priority, byte[] Payload)> ReadAllData()
    {
        return _queue.ReadAllData();
    }

    public Result Enqueue(long key, byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        foreach (var policy in _policies)
        {
            if (!policy.CanEnqueue(key, payload, this))
            {
                return Result.PolicyViolation(policy);
            }
        }

        _queue.Enqueue(key, payload);

        return Result.Success();
    }

    public bool TryDequeue(out long key, out byte[] payload)
    {
        return _queue.TryDequeue(out key, out payload);
    }
}