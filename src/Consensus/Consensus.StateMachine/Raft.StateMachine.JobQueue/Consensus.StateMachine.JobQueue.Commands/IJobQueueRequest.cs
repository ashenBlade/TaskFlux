namespace Consensus.StateMachine.JobQueue.Commands;

public interface IJobQueueRequest
{
    public RequestType Type { get; }
    public void Accept(IJobQueueRequestVisitor visitor);
}