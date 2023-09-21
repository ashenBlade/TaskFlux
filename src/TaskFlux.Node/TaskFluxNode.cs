using JobQueue.Core;
using TaskFlux.Core;

namespace TaskFlux.Node;

public class TaskFluxNode: INode
{
    private readonly IJobQueueManager _jobQueueManager;

    public TaskFluxNode(IJobQueueManager jobQueueManager)
    {
        _jobQueueManager = jobQueueManager;
    }
    
    public IJobQueueManager GetJobQueueManager()
    {
        return _jobQueueManager;
    }
}