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

    public override void Serialize(Stream stream)
    {
        var writer = new StreamBinaryWriter(stream);
        writer.Write(DeltaType.AddRecord);
        writer.Write(QueueName);
        writer.Write(Key);
        writer.WriteBuffer(Message);
    }
}