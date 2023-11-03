using TaskFlux.Core;
using TaskQueue.Core;

namespace TaskFlux.Node;

public class TaskFluxNode : INode
{
    private readonly ITaskQueueManager _manager;

    public TaskFluxNode(ITaskQueueManager manager)
    {
        _manager = manager;
    }

    public ITaskQueueManager GetTaskQueueManager()
    {
        return _manager;
    }
}