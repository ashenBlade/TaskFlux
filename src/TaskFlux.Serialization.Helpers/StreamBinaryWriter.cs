using System.Buffers.Binary;
using TaskQueue.Core;

namespace TaskFlux.Serialization.Helpers;

public struct StreamBinaryWriter
{
    public Stream Stream { get; } = Stream.Null;

    public StreamBinaryWriter(Stream stream)
    {
        Stream = stream;
    }

    public void Write(QueueName queueName)
    {
        Span<byte> temp = stackalloc byte[QueueNameParser.MaxNameLength + 1];
        temp[0] = ( byte ) queueName.Name.Length;
        var length = QueueNameParser.Encoding.GetBytes(queueName.Name, temp[1..]);
        Stream.Write(temp[..( length + 1 )]);
    }

    public void Write(uint value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        Stream.Write(buffer);
    }

    public void Write(long value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        Stream.Write(buffer);
    }

    public void Write(int value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        Stream.Write(buffer);
    }

    public void WriteBuffer(byte[] value)
    {
        // Длина
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value.Length);
        Stream.Write(buffer);

        // Данные
        Stream.Write(value);
    }

    public void Flush()
    {
        Stream.Flush();
    }

    public void Seek(long position, SeekOrigin origin)
    {
        Stream.Seek(position, origin);
    }

    /// <summary>
    /// Записать в поток сырые байты
    /// </summary>
    /// <param name="value">Массив, который нужно записать в поток</param>
    /// <remarks>
    /// Этот метод записывает массив как есть - без добавления длины буфера в начале.
    /// Если нужно добавить длину - есть <see cref="WriteBuffer"/>
    /// </remarks>
    public void Write(ReadOnlySpan<byte> value)
    {
        Stream.Write(value);
    }

    /// <summary>
    /// Записать в поток сырые байты
    /// </summary>
    /// <param name="data">Массив, который нужно записать в поток</param>
    /// <remarks>
    /// Этот метод записывает массив как есть - без добавления длины буфера в начале.
    /// Если нужно добавить длину - есть <see cref="WriteBuffer"/>
    /// </remarks>
    public void Write(ReadOnlyMemory<byte> data) => Write(data.Span);
}