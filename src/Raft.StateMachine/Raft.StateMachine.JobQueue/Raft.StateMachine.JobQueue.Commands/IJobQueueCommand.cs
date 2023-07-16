using JobQueue.Core;

namespace Raft.StateMachine.JobQueue.Commands;

public interface IJobQueueCommand
{
    public IJobQueueResponse Apply(IJobQueue jobQueue);
    public void ApplyNoResponse(IJobQueue jobQueue);
}