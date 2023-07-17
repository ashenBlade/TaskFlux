namespace Consensus.StateMachine.JobQueue.Commands.Dequeue;

public record DequeueRequest(): IJobQueueRequest
{
    public static readonly DequeueRequest Instance = new();
    public RequestType Type => RequestType.DequeueRequest;
    public void Accept(IJobQueueRequestVisitor visitor)
    {
        visitor.Visit(this);
    }
}