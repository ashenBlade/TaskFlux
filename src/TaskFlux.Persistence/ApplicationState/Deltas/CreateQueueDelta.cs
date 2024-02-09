using System.Buffers;
using TaskFlux.Core;
using TaskFlux.PriorityQueue;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Persistence.ApplicationState.Deltas;

/// <summary>
/// Создать новую очередь с указанными параметрами
/// </summary>
public class CreateQueueDelta : Delta
{
    public override DeltaType Type => DeltaType.CreateQueue;
    public QueueName QueueName { get; }
    public PriorityQueueCode Code { get; }
    public int? MaxQueueSize { get; }
    public int? MaxMessageSize { get; }
    public (long, long)? PriorityRange { get; }

    public CreateQueueDelta(QueueName queueName,
                            PriorityQueueCode code,
                            int? maxQueueSize,
                            int? maxMessageSize,
                            (long, long)? priorityRange)
    {
        QueueName = queueName;
        Code = code;
        MaxQueueSize = maxQueueSize;
        MaxMessageSize = maxMessageSize;
        PriorityRange = priorityRange;
    }

    public override byte[] Serialize()
    {
        var size = sizeof(DeltaType)                                // Маркер
                 + MemoryBinaryWriter.EstimateResultSize(QueueName) // Название очереди
                 + sizeof(int)                                      // Тип реализации очереди
                 + sizeof(int)                                      // Максимальный размер очереди
                 + sizeof(int)                                      // Максимальный размер сообщения
                 + sizeof(bool);                                    // Есть ли ограничение на диапазон ключей
        if (PriorityRange.HasValue)
        {
            size += sizeof(long) + sizeof(long); // Ограничение на диапазон ключей 
        }

        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var memory = buffer.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(DeltaType.CreateQueue);
            writer.Write(QueueName);
            writer.Write(( int ) Code);
            writer.Write(MaxQueueSize ?? -1);
            writer.Write(MaxMessageSize ?? -1);
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

            return memory.ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    internal static CreateQueueDelta Deserialize(byte[] buffer)
    {
        var reader = new SpanBinaryReader(buffer.AsSpan(1));
        var queueName = reader.ReadQueueName();
        var implementation = ( PriorityQueueCode ) reader.ReadInt32();
        int? maxQueueSize = reader.ReadInt32();
        if (maxQueueSize == -1)
        {
            maxQueueSize = null;
        }

        int? maxMessageSize = reader.ReadInt32();
        if (maxMessageSize == -1)
        {
            maxMessageSize = null;
        }

        (long, long)? priorityRange = null;
        if (reader.ReadBool())
        {
            priorityRange = ( reader.ReadInt64(), reader.ReadInt64() );
        }

        return new CreateQueueDelta(queueName, implementation, maxQueueSize, maxMessageSize, priorityRange);
    }

    public override void Apply(QueueCollection queues)
    {
        queues.CreateQueue(QueueName, Code, MaxQueueSize, MaxMessageSize, PriorityRange);
    }
}