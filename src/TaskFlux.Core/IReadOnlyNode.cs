using JobQueue.Core;

namespace TaskFlux.Core;

public interface IReadOnlyNode
{
    public IReadOnlyJobQueueManager GetJobQueueManager();
}