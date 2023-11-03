using TaskQueue.Core;

namespace TaskFlux.Core;

public interface IReadOnlyNode
{
    public IReadOnlyTaskQueueManager GetTaskQueueManager();
}