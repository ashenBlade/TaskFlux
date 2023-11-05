using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands;

public abstract class Response
{
    public abstract ResponseType Type { get; }
    public abstract void Accept(IResponseVisitor visitor);
    public abstract ValueTask AcceptAsync(IAsyncResponseVisitor visitor, CancellationToken token = default);
}