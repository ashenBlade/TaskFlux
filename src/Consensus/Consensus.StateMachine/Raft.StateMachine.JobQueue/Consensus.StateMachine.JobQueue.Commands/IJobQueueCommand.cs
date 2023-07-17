using JobQueue.Core;

namespace Consensus.StateMachine.JobQueue.Commands;

public interface IJobQueueCommand
{
    public IJobQueueResponse Apply(IJobQueue jobQueue);
    public void ApplyNoResponse(IJobQueue jobQueue);
}