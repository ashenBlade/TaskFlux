using System.Buffers;
using TaskFlux.Core;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Persistence.Delta;

public class DeleteQueueDelta : Delta
{
    public override DeltaType Type => DeltaType.DeleteQueue;
    public QueueName QueueName { get; }

    public DeleteQueueDelta(QueueName queueName)
    {
        QueueName = queueName;
    }

    public override byte[] Serialize()
    {
        var size = sizeof(DeltaType)
                 + MemoryBinaryWriter.EstimateResultSize(QueueName);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var memory = buffer.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(DeltaType.DeleteQueue);
            writer.Write(QueueName);
            return memory.ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    internal static DeleteQueueDelta Deserialize(byte[] buffer)
    {
        var reader = new SpanBinaryReader(buffer.AsSpan(1));
        return new DeleteQueueDelta(reader.ReadQueueName());
    }

    public override void Apply(QueueCollection queues)
    {
        queues.DeleteQueue(QueueName);
    }
}