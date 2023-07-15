using JobQueue.Core;
using Raft.StateMachine.JobQueue.Commands;

namespace Raft.StateMachine.JobQueue;

public interface ICommand
{
    public IJobQueueResponse Apply(IJobQueue jobQueue);
    public void ApplyNoResponse(IJobQueue jobQueue);
}