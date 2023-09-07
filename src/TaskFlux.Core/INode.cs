using JobQueue.Core;

namespace TaskFlux.Core;

public interface INode : IReadOnlyNode
{
    public new IJobQueueManager GetJobQueueManager();
    IReadOnlyJobQueueManager IReadOnlyNode.GetJobQueueManager() => GetJobQueueManager();
}