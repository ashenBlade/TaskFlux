using System.Diagnostics;
using TaskFlux.Core.Queue;

namespace TaskFlux.Core.Policies;

public class MaxPayloadSizeQueuePolicy : QueuePolicy
{
    public int MaxPayloadSize { get; }

    public MaxPayloadSizeQueuePolicy(int maxPayloadSize)
    {
        Debug.Assert(maxPayloadSize >= 0,
            "maxSize >= 0",
            "Максимальный размер сообщения не может быть отрицательным");

        MaxPayloadSize = maxPayloadSize;
    }

    internal override bool CanEnqueue(long key, byte[] payload, IReadOnlyTaskQueue queue)
    {
        Debug.Assert(queue is not null, "queue is not null", "Объект очереди не может быть null");
        Debug.Assert(payload is not null, "payload is not null", "Массив байтов сообщения не может быть null");

        if (MaxPayloadSize < payload.Length)
        {
            return false;
        }

        return true;
    }

    internal override void Enrich(TaskQueueMetadata metadata)
    {
        metadata.MaxPayloadSize = MaxPayloadSize;
    }

    public override TReturn Accept<TReturn>(IQueuePolicyVisitor<TReturn> visitor)
    {
        return visitor.Visit(this);
    }
}