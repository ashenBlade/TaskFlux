namespace Raft.StateMachine.JobQueue.Commands.Enqueue;

public record EnqueueResponse(bool Success): IJobQueueResponse
{
    public static readonly EnqueueResponse Ok = new(true);
    public static readonly EnqueueResponse Fail = new(false);
    public ResponseType Type => ResponseType.Enqueue;
    public void Accept(IJobQueueResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}