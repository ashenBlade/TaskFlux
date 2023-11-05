using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.Ok;

public class OkResponse : Response
{
    public static readonly OkResponse Instance = new();
    public override ResponseType Type => ResponseType.Ok;

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncResponseVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}