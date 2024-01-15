using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Ok;

public class OkResponse : Response
{
    public static readonly OkResponse Instance = new();
    public override ResponseType Type => ResponseType.Ok;

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    // TODO: убрать получение дельты в посетителя
    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}