namespace TaskFlux.Requests.Requests.JobQueue.Enqueue;

public record EnqueueRequest(int Key, byte[] Payload): IRequest
{
    public void Accept(IRequestVisitor visitor)
    {
        visitor.Visit(this);
    }
}