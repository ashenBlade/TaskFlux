using System.Buffers;
using TaskFlux.Core;
using TaskFlux.Core.Restore;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Persistence.ApplicationState.Deltas;

public class RemoveRecordDelta : Delta
{
    public override DeltaType Type => DeltaType.RemoveRecord;
    public QueueName QueueName { get; }
    public long Key { get; }
    public byte[] Message { get; }

    public RemoveRecordDelta(QueueName queueName, long key, byte[] message)
    {
        QueueName = queueName;
        Key = key;
        Message = message;
    }

    public override byte[] Serialize()
    {
        var bufferSize = sizeof(DeltaType)
                       + MemoryBinaryWriter.EstimateResultSize(QueueName)
                       + sizeof(long)
                       + sizeof(int)
                       + Message.Length;
        var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            var memory = buffer.AsMemory(0, bufferSize);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(DeltaType.RemoveRecord);
            writer.Write(QueueName);
            writer.Write(Key);
            writer.WriteBuffer(Message);
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
        var key = reader.ReadInt64();
        var message = reader.ReadBuffer();
        return new RemoveRecordDelta(queueName, key, message);
    }

    public override void Apply(QueueCollection queues)
    {
        queues.RemoveRecord(QueueName, Key, Message);
    }
}