using System.Buffers;
using Utils.Serialization;

namespace TaskFlux.Network.Packets.Commands;

public sealed class CountNetworkCommand : NetworkCommand
{
    public override NetworkCommandType Type => NetworkCommandType.Count;
    public string QueueName { get; }

    public CountNetworkCommand(string queueName)
    {
        QueueName = queueName;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var size = sizeof(NetworkCommandType)
                 + MemoryBinaryWriter.EstimateResultSize(QueueName);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var memory = buffer.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(NetworkCommandType.Count);
            writer.Write(QueueName);
            await stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public override T Accept<T>(INetworkCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }

    public new static async ValueTask<CountNetworkCommand> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var queueName = await reader.ReadQueueNameAsync(token);
        return new CountNetworkCommand(queueName);
    }
}