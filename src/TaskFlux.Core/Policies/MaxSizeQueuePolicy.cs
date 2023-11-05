using System.Diagnostics;
using TaskFlux.Abstractions;

namespace TaskFlux.Core.Policies;

public class MaxSizeQueuePolicy : QueuePolicy
{
    public int MaxSize { get; }

    public MaxSizeQueuePolicy(int maxSize)
    {
        if (maxSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxSize), maxSize,
                "Максимальный размер очереди не может быть отрицательным");
        }

        MaxSize = maxSize;
    }

    public override bool CanEnqueue(long key, byte[] payload, IReadOnlyTaskQueue queue)
    {
        Debug.Assert(queue is not null, "queue is not null", "Объект очереди не может быть null");

        if (MaxSize <= queue.Count)
        {
            return false;
        }

        return false;
    }


    public override void Enrich(TaskQueueMetadata metadata)
    {
        metadata.MaxSize = MaxSize;
    }
}