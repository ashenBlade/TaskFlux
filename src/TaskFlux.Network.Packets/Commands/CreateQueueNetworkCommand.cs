using System.Buffers;
using TaskFlux.Models;
using Utils.Serialization;

namespace TaskFlux.Network.Packets.Commands;

public sealed class CreateQueueNetworkCommand : NetworkCommand
{
    public QueueName QueueName { get; }
    public int Code { get; }
    public int? MaxQueueSize { get; }
    public int? MaxMessageSize { get; }
    public (long, long)? PriorityRange { get; }

    public CreateQueueNetworkCommand(QueueName queueName,
                                     int code,
                                     int? maxQueueSize,
                                     int? maxMessageSize,
                                     (long, long)? priorityRange)
    {
        QueueName = queueName;
        Code = code;
        MaxQueueSize = maxQueueSize;
        MaxMessageSize = maxMessageSize;
        PriorityRange = priorityRange;
    }

    public override NetworkCommandType Type => NetworkCommandType.CreateQueue;

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var size = sizeof(NetworkCommandType)                       // Маркер
                 + MemoryBinaryWriter.EstimateResultSize(QueueName) // Название очереди
                 + sizeof(int)                                      // Тип реализации
                 + sizeof(int)                                      // Максимальный размер очереди
                 + sizeof(int)                                      // Максимальный размер сообщения
                 + sizeof(bool);                                    // Есть ли диапазон приоритетов
        if (PriorityRange.HasValue)
        {
            size += sizeof(long)  // Минимальный ключ
                  + sizeof(long); // Максимальный ключ
        }

        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var memory = buffer.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(NetworkCommandType.CreateQueue);
            writer.Write(QueueName);
            writer.Write(Code);
            writer.Write(MaxQueueSize ?? -1);
            writer.Write(MaxMessageSize ?? -1);
            if (PriorityRange is var (min, max))
            {
                writer.Write(true);
                writer.Write(min);
                writer.Write(max);
            }
            else
            {
                writer.Write(false);
            }

            await stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public new static async ValueTask<CreateQueueNetworkCommand> DeserializeAsync(
        Stream stream,
        CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var queueName = await reader.ReadQueueNameAsync(token);
        var code = await reader.ReadInt32Async(token);
        int? maxQueueSize = await reader.ReadInt32Async(token);
        if (maxQueueSize == -1)
        {
            maxQueueSize = null;
        }

        int? maxMessageSize = await reader.ReadInt32Async(token);
        if (maxMessageSize == -1)
        {
            maxMessageSize = null;
        }

        (long, long)? priorityRange = null;
        if (await reader.ReadBoolAsync(token))
        {
            priorityRange = ( await reader.ReadInt64Async(token), await reader.ReadInt64Async(token) );
        }

        return new CreateQueueNetworkCommand(queueName, code, maxQueueSize, maxMessageSize, priorityRange);
    }

    public override T Accept<T>(INetworkCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}