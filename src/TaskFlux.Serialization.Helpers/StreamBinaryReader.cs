using System.Buffers;
using System.Buffers.Binary;
using JobQueue.Core;
using JobQueue.Core.Exceptions;

namespace TaskFlux.Serialization.Helpers;

public struct StreamBinaryReader
{
    public Stream Stream { get; } = Stream.Null;

    public StreamBinaryReader(Stream stream)
    {
        Stream = stream;
    }

    /// <summary>
    /// Прочитать из очереди сериализованное название очереди
    /// </summary>
    /// <exception cref="EndOfStreamException">Был достигнут конец очереди</exception>
    /// <exception cref="InvalidQueueNameException">Неправильное серилазованное название очереди</exception>
    /// <returns>Десериализованное название очереди</returns>
    public QueueName ReadQueueName()
    {
        var length = ReadByte();
        Span<byte> buffer = stackalloc byte[length];
        Stream.ReadExactly(buffer);
        return QueueNameParser.Parse(buffer);
    }

    private byte ReadByte()
    {
        var length = Stream.ReadByte();
        if (length == -1)
        {
            throw new EndOfStreamException("Не удалось прочитать длину названия очереди: достигнут конец потока");
        }

        return ( byte ) length;
    }

    public uint ReadUInt32()
    {
        Span<byte> buffer = stackalloc byte[sizeof(uint)];
        Stream.ReadExactly(buffer);
        return BinaryPrimitives.ReadUInt32BigEndian(buffer);
    }

    public long ReadInt64()
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        Stream.ReadExactly(buffer);
        return BinaryPrimitives.ReadInt64BigEndian(buffer);
    }

    public byte[] ReadBuffer()
    {
        var length = ReadInt32();
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            Stream.ReadExactly(buffer.AsSpan(0, length));
            var result = new byte[length];
            Array.Copy(buffer, result, length);
            return result;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public int ReadInt32()
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        Stream.ReadExactly(buffer);
        return BinaryPrimitives.ReadInt32BigEndian(buffer);
    }
}