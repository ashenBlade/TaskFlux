using JobQueue.Core;
using TaskFlux.Core;

namespace TaskFlux.Node;

public class SingleJobQueueNode: INode
{
    private readonly IJobQueue _singleJobQueue;

    public SingleJobQueueNode(IJobQueue singleJobQueue)
    {
        _singleJobQueue = singleJobQueue;
    }
    
    public IJobQueue GetJobQueue() => _singleJobQueue;
}