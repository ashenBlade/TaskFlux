using TaskQueue.Core;

namespace TaskFlux.Core;

public interface INode : IReadOnlyNode
{
    public new ITaskQueueManager GetTaskQueueManager();
    IReadOnlyTaskQueueManager IReadOnlyNode.GetTaskQueueManager() => GetTaskQueueManager();
}