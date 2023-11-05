using TaskQueue.Models;

namespace TaskQueue.Core;

public class TaskQueueMetadata : ITaskQueueMetadata
{
    private readonly ITaskQueue _parent;

    public TaskQueueMetadata(ITaskQueue parent)
    {
        _parent = parent;
    }

    public QueueName QueueName => _parent.Name;
    public int Count => _parent.Count;

    public (long Min, long Max)? PriorityRange { get; internal set; } = null;
    public int? MaxPayloadSize { get; internal set; }
    public int? MaxSize { get; internal set; }
}