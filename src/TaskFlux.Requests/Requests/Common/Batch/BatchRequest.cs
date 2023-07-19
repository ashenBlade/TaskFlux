namespace TaskFlux.Requests.Batch;

public class BatchRequest: IRequest
{
    public ICollection<IRequest> Requests { get; }
    public BatchRequest(ICollection<IRequest> requests)
    {
        Requests = requests;
    }

    public void Accept(IRequestVisitor visitor)
    {
        visitor.Visit(this);
    }
}