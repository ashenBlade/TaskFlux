using TaskFlux.Commands.Error;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.PolicyViolation;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Models;

namespace TaskFlux.Commands.Enqueue;

public class EnqueueCommand : UpdateCommand
{
    public QueueName Queue { get; }
    public long Key { get; }
    public byte[] Payload { get; }

    public EnqueueCommand(long key, byte[] payload, QueueName queue)
    {
        ArgumentNullException.ThrowIfNull(payload);
        Key = key;
        Payload = payload;
        Queue = queue;
    }

    public override CommandType Type => CommandType.Enqueue;

    public override Response Apply(IApplication application)
    {
        if (!application.TaskQueueManager.TryGetQueue(Queue, out var queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        var result = queue.Enqueue(Key, Payload);

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

        queue.Enqueue(Key, Payload);
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