using System.Diagnostics;
using TaskFlux.Core.Queue;

namespace TaskFlux.Core.Policies;

public class MaxQueueSizeQueuePolicy : QueuePolicy
{
    public int MaxQueueSize { get; }

    public MaxQueueSizeQueuePolicy(int maxQueueSize)
    {
        if (maxQueueSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxQueueSize), maxQueueSize,
                "Максимальный размер очереди не может быть отрицательным");
        }

        MaxQueueSize = maxQueueSize;
    }

    internal override bool CanEnqueue(long key, byte[] payload, IReadOnlyTaskQueue queue)
    {
        Debug.Assert(queue is not null, "queue is not null", "Объект очереди не может быть null");

        if (MaxQueueSize <= queue.Count)
        {
            return false;
        }

        return false;
    }


    internal override void Enrich(TaskQueueMetadata metadata)
    {
        metadata.MaxSize = MaxQueueSize;
    }

    public override TReturn Accept<TReturn>(IQueuePolicyVisitor<TReturn> visitor)
    {
        return visitor.Visit(this);
    }
}