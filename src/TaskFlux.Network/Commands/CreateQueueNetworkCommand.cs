using System.Buffers;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Commands;

public sealed class CreateQueueNetworkCommand : NetworkCommand
{
    public string QueueName { get; }

    /// <summary>
    /// Код реализации очереди
    /// </summary>
    public int Code { get; }

    public int? MaxQueueSize { get; }
    public int? MaxPayloadSize { get; }
    public (long, long)? PriorityRange { get; }

    public CreateQueueNetworkCommand(string queueName,
                                     int code,
                                     int? maxQueueSize,
                                     int? maxPayloadSize,
                                     (long, long)? priorityRange)
    {
        QueueName = queueName;
        Code = code;
        MaxQueueSize = maxQueueSize;
        MaxPayloadSize = maxPayloadSize;
        PriorityRange = priorityRange;
    }

    public bool TryGetPriorityRange(out long min, out long max)
    {
        ( min, max ) = PriorityRange.GetValueOrDefault();
        return PriorityRange.HasValue;
    }

    public override NetworkCommandType Type => NetworkCommandType.CreateQueue;

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var size = sizeof(NetworkCommandType)                                  // Маркер
                 + MemoryBinaryWriter.EstimateResultSizeAsQueueName(QueueName) // Название очереди
                 + sizeof(int)                                                 // Тип реализации
                 + sizeof(int)                                                 // Максимальный размер очереди
                 + sizeof(int)                                                 // Максимальный размер сообщения
                 + sizeof(bool);                                               // Есть ли диапазон приоритетов
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
            writer.WriteAsQueueName(QueueName);
            writer.Write(Code);
            writer.Write(MaxQueueSize ?? -1);
            writer.Write(MaxPayloadSize ?? -1);
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
        var queueName = await reader.ReadAsQueueNameAsync(token);
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