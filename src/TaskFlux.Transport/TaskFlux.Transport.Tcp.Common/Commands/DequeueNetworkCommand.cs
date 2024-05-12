using System.Buffers;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Commands;

public sealed class DequeueNetworkCommand : NetworkCommand
{
    public override NetworkCommandType Type => NetworkCommandType.Dequeue;

    /// <summary>
    /// Специальная константа для обозначения запроса без таймаута
    /// </summary>
    public const uint NoTimeout = 0;

    /// <summary>
    /// Очередь, из которой читать 
    /// </summary>
    public string QueueName { get; }

    /// <summary>
    /// Таймаут ожидания чтения запроса
    /// </summary>
    public uint TimeoutMs { get; }

    public DequeueNetworkCommand(string queueName, uint timeoutMs)
    {
        QueueName = queueName;
        TimeoutMs = timeoutMs;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var size = sizeof(NetworkCommandType)
                   + MemoryBinaryWriter.EstimateResultSizeAsQueueName(QueueName)
                   + sizeof(uint);

        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var memory = buffer.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(NetworkCommandType.Dequeue);
            writer.WriteAsQueueName(QueueName);
            writer.Write(TimeoutMs);
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
        var timeout = await reader.ReadUInt32Async(token);
        return new DequeueNetworkCommand(queueName, timeout);
    }

    public override T Accept<T>(INetworkCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}