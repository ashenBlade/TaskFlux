using TaskFlux.Network.Commands;

namespace TaskFlux.Client;

public class CreateQueueOptions
{
    internal (long, long)? PriorityRange { get; set; }
    internal int? MaxQueueSize { get; set; }
    internal int? MaxMessageSize { get; set; }
    internal int ImplementationCode { get; set; } = PriorityQueueCodes.Default;

    public CreateQueueOptions UseHeap()
    {
        ImplementationCode = PriorityQueueCodes.Heap;
        return this;
    }

    public CreateQueueOptions UseQueueArray(long min, long max)
    {
        CheckPriorityRange(min, max);

        PriorityRange = ( min, max );
        ImplementationCode = PriorityQueueCodes.QueueArray;
        return this;
    }

    private static void CheckPriorityRange(long min, long max)
    {
        if (max < min)
        {
            throw new ArgumentOutOfRangeException(nameof(max), ( min, max ),
                "Минимальный ключ не должен быть больше максимального");
        }
    }

    public CreateQueueOptions WithMaxQueueSize(int maxQueueSize)
    {
        if (maxQueueSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxQueueSize), maxQueueSize,
                "Максимальный размер очереди не может быть отрицательным");
        }

        MaxQueueSize = maxQueueSize;
        return this;
    }

    public CreateQueueOptions WithMaxMessageSize(int maxMessageSize)
    {
        if (maxMessageSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxMessageSize), maxMessageSize,
                "Максимальный размер сообщения не может быть отрицательным");
        }

        MaxMessageSize = maxMessageSize;
        return this;
    }

    public CreateQueueOptions WithPriorityRange(long min, long max)
    {
        CheckPriorityRange(min, max);
        PriorityRange = ( min, max );
        return this;
    }
}