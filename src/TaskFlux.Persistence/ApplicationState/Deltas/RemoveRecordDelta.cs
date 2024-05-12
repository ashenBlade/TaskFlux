using System.Buffers;
using TaskFlux.Core;
using TaskFlux.Core.Restore;
using TaskFlux.Domain;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Persistence.ApplicationState.Deltas;

public class RemoveRecordDelta : Delta
{
    public override DeltaType Type => DeltaType.RemoveRecord;
    public QueueName QueueName { get; }
    public RecordId Id { get; }

    public RemoveRecordDelta(QueueName queueName, RecordId id)
    {
        QueueName = queueName;
        Id = id;
    }

    public override byte[] Serialize()
    {
        var bufferSize = sizeof(DeltaType)
                         // Название очереди
                         + MemoryBinaryWriter.EstimateResultSize(QueueName)
                         // ID записи
                         + sizeof(ulong);

        var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            var memory = buffer.AsMemory(0, bufferSize);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(DeltaType.RemoveRecord);
            writer.Write(QueueName);
            writer.Write(Id);
            return memory.ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    internal static RemoveRecordDelta Deserialize(byte[] buffer)
    {
        var reader = new SpanBinaryReader(buffer.AsSpan(1));
        var queueName = reader.ReadQueueName();
        var id = reader.ReadUInt64();
        return new RemoveRecordDelta(queueName, new RecordId(id));
    }

    public override void Apply(QueueCollection queues)
    {
        queues.RemoveRecord(QueueName, Id);
    }
}