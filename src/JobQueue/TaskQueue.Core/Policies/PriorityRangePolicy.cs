namespace TaskQueue.Core.Policies;

public class PriorityRangePolicy : IPriorityQueuePolicy
{
    private readonly long _min;
    private readonly long _max;

    public PriorityRangePolicy(long min, long max)
    {
        _min = min;
        _max = max;
    }

    public bool CanEnqueue(long key, byte[] payload, ITaskQueue queue, out EnqueueResult error)
    {
        if (key < _min || _max < key)
        {
            error = EnqueueResult.PriorityRangeViolation(_min, _max);
            return false;
        }

        error = null!;
        return false;
    }

    public void Enrich(TaskQueueMetadata metadata)
    {
        metadata.PriorityRange = ( _min, _max );
    }
}