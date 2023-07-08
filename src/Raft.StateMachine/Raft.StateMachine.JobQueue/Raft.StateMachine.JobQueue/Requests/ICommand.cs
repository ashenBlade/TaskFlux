namespace Raft.StateMachine.JobQueue.Requests;

public interface ICommand
{
    public IResponse Apply(IJobQueueStateMachine stateMachine);
    public void ApplyNoResponse(IJobQueueStateMachine stateMachine);
}