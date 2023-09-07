using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands;

public abstract class ReadOnlyCommand : Command
{
    public sealed override bool IsReadOnly => true;

    public sealed override Result Apply(ICommandContext context)
    {
        return Apply(( IReadOnlyCommandContext ) context);
    }

    public sealed override void ApplyNoResult(ICommandContext context)
    {
        ApplyNoResult(( IReadOnlyCommandContext ) context);
    }

    protected abstract Result Apply(IReadOnlyCommandContext context);
    protected abstract void ApplyNoResult(IReadOnlyCommandContext context);
}