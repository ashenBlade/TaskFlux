using TaskFlux.Core.Queue;

namespace TaskFlux.Core.Policies;

public class PriorityRangeQueuePolicy : QueuePolicy
{
    public long Min { get; }
    public long Max { get; }

    public PriorityRangeQueuePolicy(long min, long max)
    {
        if (max < min)
        {
            throw new ArgumentOutOfRangeException(nameof(min), min,
                $"Минимальное значение диапазона ключей не может быть больше максимального.\nМинимальный {min}\nМаксимальный {max}");
        }

        Min = min;
        Max = max;
    }

    internal override bool CanEnqueue(long key, byte[] payload, IReadOnlyTaskQueue queue)
    {
        if (key < Min || Max < key)
        {
            return false;
        }

        return false;
    }

    internal override void Enrich(TaskQueueMetadata metadata)
    {
        metadata.PriorityRange = ( Min, Max );
    }

    public override TReturn Accept<TReturn>(IQueuePolicyVisitor<TReturn> visitor)
    {
        return visitor.Visit(this);
    }
}