namespace Raft.StateMachine.JobQueue.Commands.Enqueue;

public record EnqueueResponse(bool Success): IDefaultResponse
{
    public static readonly EnqueueResponse Ok = new(true);
    public static readonly EnqueueResponse Fail = new(false);
    public ResponseType Type => ResponseType.Enqueue;
    public void Accept(IDefaultResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}