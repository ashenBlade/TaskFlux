using System.Buffers;
using TaskFlux.Models;
using Utils.Serialization;

namespace TaskFlux.Delta;

public class AddRecordDelta : Delta
{
    public AddRecordDelta(QueueName queueName, long key, byte[] message)
    {
        QueueName = queueName;
        Key = key;
        Message = message;
    }

    public override DeltaType Type => DeltaType.AddRecord;
    public QueueName QueueName { get; }
    public long Key { get; }
    public byte[] Message { get; }

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
            writer.Write(DeltaType.AddRecord);
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
}