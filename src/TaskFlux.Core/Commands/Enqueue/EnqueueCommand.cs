using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.PolicyViolation;
using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Enqueue;

public class EnqueueCommand : ModificationCommand
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

        return new EnqueueResponse(Queue, Key, Message);
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