namespace Raft.StateMachine.JobQueue.Commands;

public interface IDefaultResponse
{
    public ResponseType Type { get; }
    public void Accept(IDefaultResponseVisitor visitor);
}