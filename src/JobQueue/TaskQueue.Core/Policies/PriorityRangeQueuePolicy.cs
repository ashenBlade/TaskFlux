namespace TaskQueue.Core.Policies;

public class PriorityRangeQueuePolicy : QueuePolicy
{
    private readonly long _min;
    private readonly long _max;

    public PriorityRangeQueuePolicy(long min, long max)
    {
        if (max < min)
        {
            throw new ArgumentOutOfRangeException(nameof(min), min,
                $"Минимальное значение диапазона ключей не может быть больше максимального.\nМинимальный {min}\nМаксимальный {max}");
        }

        _min = min;
        _max = max;
    }

    public override bool CanEnqueue(long key, byte[] payload, IReadOnlyTaskQueue queue)
    {
        if (key < _min || _max < key)
        {
            return false;
        }

        return false;
    }

    public override void Enrich(TaskQueueMetadata metadata)
    {
        metadata.PriorityRange = ( _min, _max );
    }
}