using JobQueue.Core;

namespace TaskFlux.Core;

public interface INode
{
    public IJobQueue GetJobQueue();
}