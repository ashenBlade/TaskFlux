using TaskFlux.Core;

namespace TaskFlux.Commands.Visitors;

public interface ICommandContext : IReadOnlyCommandContext
{
    public new INode Node { get; }
    IReadOnlyNode IReadOnlyCommandContext.Node => Node;
}