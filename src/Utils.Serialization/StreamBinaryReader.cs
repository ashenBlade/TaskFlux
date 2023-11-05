using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using TaskFlux.Models;
using TaskFlux.Models.Exceptions;

namespace Utils.Serialization;

public struct StreamBinaryReader
{
    public Stream Stream { get; } = Stream.Null;
    public bool IsEnd => CheckEnd();

    private bool CheckEnd()
    {
        Debug.Assert(Stream.CanSeek,
            "Чтобы проверять достижение конца потока, поток должен поддерживать Seek операцию");
        var read = Stream.ReadByte();
        if (read == -1)
        {
            return true;
        }

        Stream.Seek(-1, SeekOrigin.Current);
        return false;
    }

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

    /// <summary>
    /// Прочитать <see cref="long"/> из потока с текущей позиции
    /// </summary>
    /// <returns>Десериализованный <see cref="long"/></returns>
    /// <exception cref="EndOfStreamException">В потоке не было нужного количества байт для <see cref="long"/></exception>
    public long ReadInt64()
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        Stream.ReadExactly(buffer);
        return BinaryPrimitives.ReadInt64BigEndian(buffer);
    }

    /// <summary>
    /// Прочитать сериализованный массив из потока с текущей позиции
    /// </summary>
    /// <returns>Десериализованный массив</returns>
    /// <exception cref="EndOfStreamException"></exception>
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

    public bool ReadBool()
    {
        var data = Stream.ReadByte();
        if (data == -1)
        {
            throw new EndOfStreamException("Не удалось прочитать Bool - достигнут конец потока");
        }

        return ( byte ) data == 1;
    }
}