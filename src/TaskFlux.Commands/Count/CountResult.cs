using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.Count;

public class CountResult : Result
{
    public static readonly CountResult Empty = new(0);
    public int Count { get; }

    public CountResult(int count)
    {
        Count = count;
    }

    public override ResultType Type => ResultType.Count;

    public override void Accept(IResultVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncResultVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}