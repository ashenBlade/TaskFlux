using JobQueue.Core;
using TaskFlux.Core;

namespace TaskFlux.Node;

public class ProxyConsensusNode: INode
{
    private readonly IJobQueue _singleJobQueue;

    public ProxyConsensusNode(IJobQueue singleJobQueue)
    {
        _singleJobQueue = singleJobQueue;
    }

    public INodeInfo NodeInfo => new PocoNodeInfo(1);

    public IJobQueue GetJobQueue() => _singleJobQueue;
}