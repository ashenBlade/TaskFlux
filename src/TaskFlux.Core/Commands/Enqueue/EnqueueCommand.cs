using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.PolicyViolation;
using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Enqueue;

public class EnqueueCommand : ModificationCommand
{
    public QueueName Queue { get; }
    public long Priority { get; }
    public byte[] Payload { get; }

    public EnqueueCommand(long priority, byte[] payload, QueueName queue)
    {
        ArgumentNullException.ThrowIfNull(payload);

        Priority = priority;
        Payload = payload;
        Queue = queue;
    }

    public override Response Apply(IApplication application)
    {
        if (!application.TaskQueueManager.TryGetQueue(Queue, out var queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        var result = queue.Enqueue(Priority, Payload);

        if (result.TryGetViolatedPolicy(out var policy))
        {
            return new PolicyViolationResponse(policy);
        }

        return new EnqueueResponse(Queue, Priority, Payload);
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