using TaskFlux.Models;
using Utils.Serialization;

namespace TaskFlux.Delta;

/// <summary>
/// Создать новую очередь с указанными параметрами
/// </summary>
public class CreateQueueDelta : Delta
{
    public override DeltaType Type => DeltaType.CreateQueue;
    public QueueName QueueName { get; }
    public int ImplementationType { get; }
    public int MaxQueueSize { get; }
    public int MaxMessageSize { get; }
    public (long, long)? PriorityRange { get; }

    public CreateQueueDelta(QueueName queueName,
                            int implementationType,
                            int maxQueueSize,
                            int maxMessageSize,
                            (long, long)? priorityRange)
    {
        QueueName = queueName;
        ImplementationType = implementationType;
        MaxQueueSize = maxQueueSize;
        MaxMessageSize = maxMessageSize;
        PriorityRange = priorityRange;
    }

    public override void Serialize(Stream stream)
    {
        var writer = new StreamBinaryWriter(stream);
        writer.Write(DeltaType.CreateQueue);
        writer.Write(QueueName);
        writer.Write(ImplementationType);
        writer.Write(MaxQueueSize);
        writer.Write(MaxMessageSize);
        if (PriorityRange is var (min, max))
        {
            writer.Write(true);
            writer.Write(min);
            writer.Write(max);
        }
        else
        {
            writer.Write(false);
        }
    }
}