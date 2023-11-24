using TaskFlux.Models;
using Utils.Serialization;

namespace TaskFlux.Delta;

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

    public override void Serialize(Stream stream)
    {
        var writer = new StreamBinaryWriter(stream);
        writer.Write(DeltaType.RemoveRecord);
        writer.Write(QueueName);
        writer.Write(Key);
        writer.WriteBuffer(Message);
    }
}