using TaskFlux.Commands.Error;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.PolicyViolation;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Models;
using TaskFlux.Serialization;

namespace TaskFlux.Commands.Enqueue;

public class EnqueueCommand : UpdateCommand
{
    public QueueName Queue { get; }
    public long Key { get; }
    public byte[] Message { get; }

    public EnqueueCommand(long key, byte[] message, QueueName queue)
    {
        ArgumentNullException.ThrowIfNull(message);
        Key = key;
        Message = message;
        Queue = queue;
    }

    public override CommandType Type => CommandType.Enqueue;

    public override Response Apply(IApplication application)
    {
        if (!application.TaskQueueManager.TryGetQueue(Queue, out var queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        var result = queue.Enqueue(Key, Message);

        if (result.TryGetViolatedPolicy(out var policy))
        {
            return new PolicyViolationResponse(policy);
        }

        return OkResponse.Instance;
    }

    public override void ApplyNoResult(IApplication context)
    {
        if (!context.TaskQueueManager.TryGetQueue(Queue, out var queue))
        {
            return;
        }

        queue.Enqueue(Key, Message);
    }

    public override bool TryGetDelta(out Delta delta)
    {
        delta = new AddRecordDelta(Queue, Key, Message);
        return true;
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