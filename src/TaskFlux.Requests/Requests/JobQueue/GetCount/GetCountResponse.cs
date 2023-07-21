namespace TaskFlux.Requests.Requests.JobQueue.GetCount;

public record GetCountResponse(int Count): IResponse
{
    public void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public static readonly GetCountResponse Empty = new(0);
}
