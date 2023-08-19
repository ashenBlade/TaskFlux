using JobQueue.Core;
using TaskFlux.Core;

namespace TaskFlux.Node;

public class SingleJobQueueNode: INode
{
    private readonly IJobQueueManager _jobQueueManager;

    public SingleJobQueueNode(IJobQueueManager jobQueueManager)
    {
        _jobQueueManager = jobQueueManager;
    }
    
    public IJobQueueManager GetJobQueueManager()
    {
        return _jobQueueManager;
    }
}