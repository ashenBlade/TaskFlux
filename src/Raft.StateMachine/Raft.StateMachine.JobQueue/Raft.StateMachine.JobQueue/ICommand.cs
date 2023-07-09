using JobQueue.Core;

namespace Raft.StateMachine.JobQueue;

public interface ICommand
{
    public JobQueueResponse Apply(IJobQueue jobQueue);
    public void ApplyNoResponse(IJobQueue jobQueue);
}