using TaskFlux.Commands.Error;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Models;

namespace TaskFlux.Commands.Dequeue;

public class DequeueCommand : UpdateCommand
{
    public QueueName Queue { get; }

    public DequeueCommand(QueueName queue)
    {
        Queue = queue;
    }

    public override CommandType Type => CommandType.Dequeue;

    public override Result Apply(IApplication context)
    {
        var manager = context.TaskQueueManager;

        if (!manager.TryGetQueue(Queue, out var queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        if (queue.TryDequeue(out var key, out var payload))
        {
            return DequeueResult.Create(key, payload);
        }

        return DequeueResult.Empty;
    }

    public override void ApplyNoResult(IApplication context)
    {
        var manager = context.TaskQueueManager;

        if (!manager.TryGetQueue(Queue, out var queue))
        {
            return;
        }

        queue.TryDequeue(out _, out _);
    }

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