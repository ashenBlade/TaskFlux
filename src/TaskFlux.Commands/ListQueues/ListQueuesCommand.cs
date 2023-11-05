using TaskFlux.Abstractions;
using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.ListQueues;

public class ListQueuesCommand : ReadOnlyCommand
{
    public override CommandType Type => CommandType.ListQueues;
    public static readonly ListQueuesCommand Instance = new();

    protected override Response Apply(IReadOnlyApplication context)
    {
        var result = context.TaskQueueManager.GetAllQueuesMetadata();
        return new ListQueuesResponse(result);
    }

    protected override void ApplyNoResult(IReadOnlyApplication context)
    {
    }

    public override void Accept(ICommandVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(IReturningCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncCommandVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}