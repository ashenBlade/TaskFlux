using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.Enqueue;

public class EnqueueResult: Result
{
    public static readonly EnqueueResult Ok = new(true);
    public static readonly EnqueueResult Full = new(false);
    public override ResultType Type => ResultType.Enqueue;
    public bool Success { get; }
    public EnqueueResult(bool success)
    {
        Success = success;
    }

    public override void Accept(IResultVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncResultVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}