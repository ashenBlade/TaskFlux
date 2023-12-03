using System.Buffers;
using TaskFlux.Models;
using Utils.Serialization;

namespace TaskFlux.Network.Packets.Commands;

public sealed class DequeueNetworkCommand : NetworkCommand
{
    public override NetworkCommandType Type => NetworkCommandType.Dequeue;
    public QueueName QueueName { get; }

    public DequeueNetworkCommand(QueueName queueName)
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
            writer.Write(NetworkCommandType.Dequeue);
            writer.Write(QueueName);
            await stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public new static async ValueTask<DequeueNetworkCommand> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var queueName = await reader.ReadQueueNameAsync(token);
        return new DequeueNetworkCommand(queueName);
    }

    public override T Accept<T>(INetworkCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}