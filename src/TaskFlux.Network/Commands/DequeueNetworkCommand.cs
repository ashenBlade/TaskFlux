using System.Buffers;
using Utils.Serialization;

namespace TaskFlux.Network.Commands;

public sealed class DequeueNetworkCommand : NetworkCommand
{
    public override NetworkCommandType Type => NetworkCommandType.Dequeue;
    public string QueueName { get; }

    public DequeueNetworkCommand(string queueName)
    {
        QueueName = queueName;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var size = sizeof(NetworkCommandType)
                 + MemoryBinaryWriter.EstimateResultSizeAsQueueName(QueueName);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var memory = buffer.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(NetworkCommandType.Dequeue);
            writer.WriteAsQueueName(QueueName);
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
        var queueName = await reader.ReadAsQueueNameAsync(token);
        return new DequeueNetworkCommand(queueName);
    }

    public override T Accept<T>(INetworkCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}