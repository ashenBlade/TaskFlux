using TaskFlux.Commands.Error;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Models;
using TaskFlux.Serialization;

namespace TaskFlux.Commands.Dequeue;

// TODO: логика работы Dequeue Command
public class DequeueCommand : UpdateCommand
{
    public QueueName Queue { get; }
    private DequeueResponse? _response;

    public DequeueCommand(QueueName queue)
    {
        Queue = queue;
    }

    public override CommandType Type => CommandType.Dequeue;

    public override Response Apply(IApplication context)
    {
        if (_response is { } r)
        {
            return r;
        }

        var manager = context.TaskQueueManager;

        if (!manager.TryGetQueue(Queue, out var queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        if (queue.TryDequeue(out var key, out var payload))
        {
            return _response = DequeueResponse.Create(Queue, key, payload);
        }

        return _response = DequeueResponse.Empty;
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

    public override bool TryGetDelta(out Delta delta)
    {
        if (_response is {Key: var key, Message: var payload})
        {
            delta = new RemoveRecordDelta(Queue, key, payload);
            return true;
        }

        delta = default!;
        return false;
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