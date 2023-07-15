namespace Raft.StateMachine.JobQueue.Commands;

public interface IJobQueueResponse
{
    public ResponseType Type { get; }
    public void Accept(IJobQueueResponseVisitor visitor);
}