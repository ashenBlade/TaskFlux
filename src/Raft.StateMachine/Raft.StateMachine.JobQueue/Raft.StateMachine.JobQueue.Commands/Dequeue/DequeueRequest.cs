namespace Raft.StateMachine.JobQueue.Commands.Dequeue;

public record DequeueRequest(): IDefaultRequest
{
    public static readonly DequeueRequest Instance = new();
    public RequestType Type => RequestType.DequeueRequest;
    public void Accept(IDefaultRequestVisitor visitor)
    {
        visitor.Visit(this);
    }
}