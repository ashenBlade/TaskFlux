namespace Raft.StateMachine.JobQueue.Commands;

public interface IJobQueueCommandResponse: ICommandResponse
{
    public IJobQueueResponse Response { get; }
}