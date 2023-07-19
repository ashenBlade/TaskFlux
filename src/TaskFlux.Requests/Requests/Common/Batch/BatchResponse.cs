namespace TaskFlux.Requests.Batch;

public class BatchResponse: IResponse
{
    public ICollection<IResponse> Responses { get; }
    public static readonly BatchResponse Empty = new BatchResponse(Array.Empty<IResponse>());

    public BatchResponse(ICollection<IResponse> responses)
    {
        Responses = responses;
    }

    public void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}