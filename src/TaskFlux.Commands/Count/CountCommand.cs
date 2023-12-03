using TaskFlux.Commands.Error;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Models;

namespace TaskFlux.Commands.Count;

public class CountCommand : ReadOnlyCommand
{
    public QueueName Queue { get; }
    public override CommandType Type => CommandType.Count;

    public CountCommand(QueueName queue)
    {
        Queue = queue;
    }

    protected override Response Apply(IReadOnlyApplication context)
    {
        var manager = context.TaskQueueManager;

        if (!manager.TryGetQueue(Queue, out var queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        var count = queue.Count;
        if (count == 0)
        {
            return CountResponse.Empty;
        }

        return new CountResponse(count);
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