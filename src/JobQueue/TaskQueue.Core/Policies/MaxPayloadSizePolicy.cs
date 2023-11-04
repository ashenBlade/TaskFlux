namespace TaskQueue.Core.Policies;

public class MaxPayloadSizePolicy : IPriorityQueuePolicy
{
    public uint MaxSize { get; }

    public MaxPayloadSizePolicy(uint maxSize)
    {
        MaxSize = maxSize;
    }

    public bool CanEnqueue(long key, byte[] payload, ITaskQueue queue, out EnqueueResult error)
    {
        if (MaxSize < payload.Length)
        {
            error = EnqueueResult.MaxPayloadSizeViolation(MaxSize);
            return false;
        }

        error = null!;
        return true;
    }

    public void Enrich(TaskQueueMetadata metadata)
    {
        metadata.MaxPayloadSize = MaxSize;
    }
}