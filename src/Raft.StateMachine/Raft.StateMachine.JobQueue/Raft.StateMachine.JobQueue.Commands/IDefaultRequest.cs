namespace Raft.StateMachine.JobQueue.Commands;

public interface IDefaultRequest
{
    public RequestType Type { get; }
    public void Accept(IDefaultRequestVisitor visitor);
}