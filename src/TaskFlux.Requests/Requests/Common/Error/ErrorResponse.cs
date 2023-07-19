namespace TaskFlux.Requests.Error;

public record ErrorResponse(string Message): IResponse
{
    public static readonly ErrorResponse EmptyMessage = new(string.Empty);
    public void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}