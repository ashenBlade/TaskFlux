using TaskFlux.Core.Policies;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Core.Queue;

internal class TaskQueue : ITaskQueue
{
    public QueueName Name { get; }

    public PriorityQueueCode Code => _queue.Code;
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

    public TaskQueue(QueueName name, IPriorityQueue queue, QueuePolicy[] policies)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(policies);

        _policies = policies;
        _queue = queue;
        Name = name;
    }

    private readonly QueuePolicy[] _policies;
    private readonly IPriorityQueue _queue;

    public IReadOnlyCollection<(long Priority, byte[] Payload)> ReadAllData()
    {
        return _queue.ReadAllData();
    }

    public EnqueueResult Enqueue(long key, byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        foreach (var policy in _policies)
        {
            if (!policy.CanEnqueue(key, payload, this))
            {
                return EnqueueResult.PolicyViolation(policy);
            }
        }

        _queue.Enqueue(key, payload);

        return EnqueueResult.Success();
    }


    public bool TryDequeue(out long key, out byte[] payload)
    {
        return _queue.TryDequeue(out key, out payload);
    }
}