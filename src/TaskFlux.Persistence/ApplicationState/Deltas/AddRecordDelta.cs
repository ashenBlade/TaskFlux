using System.Buffers;
using System.Diagnostics;
using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Core.Restore;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Persistence.ApplicationState.Deltas;

public class AddRecordDelta : Delta
{
    public AddRecordDelta(QueueName queueName, long priority, byte[] payload)
    {
        Debug.Assert(payload is not null, "payload is not null");
        QueueName = queueName;
        Priority = priority;
        Payload = payload;
    }

    public AddRecordDelta(QueueName queueName, QueueRecordData data)
    {
        QueueName = queueName;
        Priority = data.Priority;
        Payload = data.Payload;
    }

    public override DeltaType Type => DeltaType.AddRecord;
    public QueueName QueueName { get; }
    public long Priority { get; }
    public byte[] Payload { get; }

    public override byte[] Serialize()
    {
        var bufferSize = sizeof(DeltaType)
                         // Название очереди
                       + MemoryBinaryWriter.EstimateResultSize(QueueName)
                         // Приоритет
                       + sizeof(long)
                         // Данные записи
                       + sizeof(int)
                       + Payload.Length;

        var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            var memory = buffer.AsMemory(0, bufferSize);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(DeltaType.AddRecord);
            writer.Write(QueueName);
            writer.Write(Priority);
            writer.WriteBuffer(Payload);
            return memory.ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    internal static AddRecordDelta Deserialize(byte[] buffer)
    {
        var reader = new SpanBinaryReader(buffer.AsSpan(1));
        var queueName = reader.ReadQueueName();
        var priority = reader.ReadInt64();
        var payload = reader.ReadBuffer();
        return new AddRecordDelta(queueName, priority, payload);
    }

    public override void Apply(QueueCollection queues)
    {
        queues.AddRecord(QueueName, Priority, Payload);
    }
}