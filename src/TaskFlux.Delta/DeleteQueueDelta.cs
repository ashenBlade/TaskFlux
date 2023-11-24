using TaskFlux.Models;
using Utils.Serialization;

namespace TaskFlux.Delta;

public class DeleteQueueDelta : Delta
{
    public override DeltaType Type => DeltaType.DeleteQueue;
    public QueueName QueueName { get; }

    public DeleteQueueDelta(QueueName queueName)
    {
        QueueName = queueName;
    }

    public override void Serialize(Stream stream)
    {
        var writer = new StreamBinaryWriter(stream);
        writer.Write(DeltaType.DeleteQueue);
        writer.Write(QueueName);
    }
}