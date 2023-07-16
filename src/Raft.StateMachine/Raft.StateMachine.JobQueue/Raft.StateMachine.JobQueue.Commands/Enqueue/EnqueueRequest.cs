namespace Raft.StateMachine.JobQueue.Commands.Enqueue;

public record EnqueueRequest(int Key, byte[] Payload): IJobQueueRequest
{
    public RequestType Type => RequestType.EnqueueRequest;
    public void Accept(IJobQueueRequestVisitor visitor)
    {
        visitor.Visit(this);
    }
}