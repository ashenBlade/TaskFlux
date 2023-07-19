namespace TaskFlux.Requests.Enqueue;

public record EnqueueResponse(bool Success): IResponse
{
    public static readonly EnqueueResponse Ok = new(true);
    public static readonly EnqueueResponse Fail = new(false);
    public void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}