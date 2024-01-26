using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.ListQueues;

public class ListQueuesCommand : ReadOnlyCommand
{
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

    public override T Accept<T>(ICommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}