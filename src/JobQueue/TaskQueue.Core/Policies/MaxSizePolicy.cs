namespace TaskQueue.Core.Policies;

public class MaxSizePolicy : IPriorityQueuePolicy
{
    public int MaxSize { get; }

    public MaxSizePolicy(int maxSize)
    {
        MaxSize = maxSize;
    }

    public bool CanEnqueue(long key, byte[] payload, ITaskQueue queue, out EnqueueResult error)
    {
        if (MaxSize <= queue.Count)
        {
            error = EnqueueResult.MaxSizeExceeded(MaxSize);
            return false;
        }

        error = default!;
        return false;
    }

    public void Enrich(TaskQueueMetadata metadata)
    {
        metadata.MaxSize = MaxSize;
    }
}