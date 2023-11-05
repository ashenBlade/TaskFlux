using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.Count;

public class CountResponse : Response
{
    public static readonly CountResponse Empty = new(0);
    public int Count { get; }

    public CountResponse(int count)
    {
        Count = count;
    }

    public override ResponseType Type => ResponseType.Count;

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncResponseVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}