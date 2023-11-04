using System.Diagnostics;

namespace TaskQueue.Core.Policies;

public class MaxPayloadSizeQueuePolicy : QueuePolicy
{
    public int MaxSize { get; }

    public MaxPayloadSizeQueuePolicy(int maxSize)
    {
        Debug.Assert(maxSize >= 0,
            "maxSize >= 0",
            "Максимальный размер сообщения не может быть отрицательным");

        MaxSize = maxSize;
    }

    public override bool CanEnqueue(long key, byte[] payload, IReadOnlyTaskQueue queue)
    {
        Debug.Assert(queue is not null, "queue is not null", "Объект очереди не может быть null");
        Debug.Assert(payload is not null, "payload is not null", "Массив байтов сообщения не может быть null");

        if (MaxSize < payload.Length)
        {
            return false;
        }

        return true;
    }

    public override void Enrich(TaskQueueMetadata metadata)
    {
        metadata.MaxPayloadSize = MaxSize;
    }
}