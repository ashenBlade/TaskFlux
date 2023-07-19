using JobQueue.Core;
using TaskFlux.Core;
using TaskFlux.Requests;

namespace Consensus.StateMachine.JobQueue.Serializer;

public abstract class JobQueueCommand: ICommand
{
    public IResponse Apply(INode node)
    {
        var queue = node.GetJobQueue();
        return Apply(queue);
    }

    protected abstract IResponse Apply(IJobQueue jobQueue);

    public void ApplyNoResponse(INode node)
    {
        var queue = node.GetJobQueue();
        ApplyNoResponse(queue);
    }
    protected abstract void ApplyNoResponse(IJobQueue jobQueue);
}