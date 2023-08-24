using System.Buffers.Binary;
using JobQueue.Core;

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
}