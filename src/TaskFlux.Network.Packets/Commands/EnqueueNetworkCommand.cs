using System.Buffers;
using TaskFlux.Models;
using Utils.Serialization;

namespace TaskFlux.Network.Packets.Commands;

public sealed class EnqueueNetworkCommand : NetworkCommand
{
    public override NetworkCommandType Type => NetworkCommandType.Enqueue;
    public QueueName QueueName { get; }
    public long Key { get; }
    public byte[] Message { get; }

    public EnqueueNetworkCommand(QueueName queueName, long key, byte[] message)
    {
        QueueName = queueName;
        Key = key;
        Message = message;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        /*
         * Мы не знаем каких размеров будет буфер с пользовательскими данными.
         * Чтобы не аллоцировать слишком много памяти, отправим его отдельно
         */
        var size = sizeof(NetworkCommandType)                       // Маркер
                 + MemoryBinaryWriter.EstimateResultSize(QueueName) // Название очереди
                 + sizeof(long)                                     // Приоритет
                 + sizeof(int);                                     // Размер буфера
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var memory = buffer.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(NetworkCommandType.Enqueue);
            writer.Write(QueueName);
            writer.Write(Key);
            writer.Write(Message.Length);
            await stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        if (Message.Length == 0)
        {
            return;
        }

        await stream.WriteAsync(Message, token);
    }

    public new static async ValueTask<EnqueueNetworkCommand> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var queueName = await reader.ReadQueueNameAsync(token);
        var key = await reader.ReadInt64Async(token);
        var message = await reader.ReadBufferAsync(token);
        return new EnqueueNetworkCommand(queueName, key, message);
    }

    public override T Accept<T>(INetworkCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}