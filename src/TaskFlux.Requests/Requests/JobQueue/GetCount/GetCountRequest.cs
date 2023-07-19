namespace TaskFlux.Requests.GetCount;

public record GetCountRequest(): IRequest
{
    public static readonly IRequest Instance = new GetCountRequest();
    
    public void Accept(IRequestVisitor visitor)
    {
        visitor.Visit(this);
    }
}