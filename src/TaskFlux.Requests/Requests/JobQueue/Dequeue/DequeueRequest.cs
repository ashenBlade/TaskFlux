namespace TaskFlux.Requests.Dequeue;

public record DequeueRequest(): IRequest
{
    public static readonly DequeueRequest Instance = new();
    public void Accept(IRequestVisitor visitor)
    {
        visitor.Visit(this);
    }
}