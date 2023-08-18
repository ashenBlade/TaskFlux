using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.Ok;

public class OkResult: Result
{
    public static readonly OkResult Instance = new();
    public override ResultType Type => ResultType.Ok;

    public override void Accept(IResultVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncResultVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}