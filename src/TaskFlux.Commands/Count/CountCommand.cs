using TaskFlux.Core;

namespace TaskFlux.Commands.Count;

public class CountCommand: Command
{
    public static readonly CountCommand Instance = new();
    public override CommandType Type => CommandType.Count;
    public override Result Apply(ICommandContext context)
    {
        var count = context.Node.GetJobQueue().Count;
        if (count == 0)
        {
            return CountResult.Empty;
        }

        return new CountResult(count);
    }

    public override void ApplyNoResult(ICommandContext context)
    { }

    public override void Accept(ICommandVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncCommandVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }

    public override T Accept<T>(IReturningCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}