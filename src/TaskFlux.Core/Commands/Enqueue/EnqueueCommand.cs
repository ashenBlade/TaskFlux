using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.PolicyViolation;
using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Queue;

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

        if (TryGetViolatedPolicy(queue, Priority, Payload, out var violatedPolicy))
        {
            return new PolicyViolationResponse(violatedPolicy);
        }

        var record = queue.Enqueue(Priority, Payload);

        return new EnqueueResponse(Queue, record);
    }

    private bool TryGetViolatedPolicy(ITaskQueue queue, long priority, byte[] payload, out QueuePolicy violatedPolicy)
    {
        if (queue.Policies is {Count: > 0} policies)
        {
            foreach (var policy in policies)
            {
                if (!policy.CanEnqueue(priority, payload, queue))
                {
                    violatedPolicy = policy;
                    return true;
                }
            }
        }

        violatedPolicy = default!;
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