using TaskFlux.PriorityQueue;

namespace TaskFlux.Core.Queue;

public class TaskQueueMetadata : ITaskQueueMetadata
{
    private readonly ITaskQueue _parent;

    public TaskQueueMetadata(ITaskQueue parent)
    {
        _parent = parent;
    }

    public QueueName QueueName => _parent.Name;
    public PriorityQueueCode Code => _parent.Code;
    public int Count => _parent.Count;

    public (long Min, long Max)? PriorityRange { get; internal set; } = null;
    public int? MaxPayloadSize { get; internal set; }
    public int? MaxQueueSize { get; internal set; }
}