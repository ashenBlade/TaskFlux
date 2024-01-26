using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Count;

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

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}