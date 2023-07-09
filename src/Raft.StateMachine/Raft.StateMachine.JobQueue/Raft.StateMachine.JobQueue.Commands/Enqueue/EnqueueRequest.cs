namespace Raft.StateMachine.JobQueue.Commands.Enqueue;

public record EnqueueRequest(int Key, byte[] Payload): IDefaultRequest
{
    public RequestType Type => RequestType.EnqueueRequest;
    public void Accept(IDefaultRequestVisitor visitor)
    {
        visitor.Visit(this);
    }
}