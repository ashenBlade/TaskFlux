using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands;

public abstract class Result
{
    public abstract ResultType Type { get; }
    public abstract void Accept(IResultVisitor visitor);
    public abstract ValueTask AcceptAsync(IAsyncResultVisitor visitor, CancellationToken token = default);
}