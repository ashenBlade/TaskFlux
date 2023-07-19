using JobQueue.Core;

namespace TaskFlux.Core;

public interface INode
{
    public INodeInfo NodeInfo { get; }
    public IJobQueue GetJobQueue();
}