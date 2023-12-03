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

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}