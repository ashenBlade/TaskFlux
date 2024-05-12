using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using TaskFlux.Core;
using TaskFlux.Domain;

namespace TaskFlux.Utils.Serialization;

public readonly struct StreamBinaryReader
{
    private Stream Stream { get; } = Stream.Null;

    public StreamBinaryReader(Stream stream)
    {
        Stream = stream;
    }

    /// <summary>
    /// Прочитать из очереди сериализованное название очереди
    /// </summary>
    /// <exception cref="EndOfStreamException">Был достигнут конец очереди</exception>
    /// <exception cref="InvalidQueueNameException">Неправильное сериализованное название очереди</exception>
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

        return (byte)length;
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

        return (byte)data == 1;
    }

    public async Task<QueueName> ReadQueueNameAsync(CancellationToken token)
    {
        var length = await ReadByteAsync(token);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var memory = buffer.AsMemory(0, length);
            await Stream.ReadExactlyAsync(memory, token);
            return QueueNameParser.Parse(memory.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Прочитать из потока сырую строку названия очереди - без проверки корректности
    /// </summary>
    /// <param name="token"></param>
    /// <returns></returns>
    public async Task<string> ReadAsQueueNameAsync(CancellationToken token)
    {
        var length = await ReadByteAsync(token);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var memory = buffer.AsMemory(0, length);
            await Stream.ReadExactlyAsync(memory, token);
            return Encoding.ASCII.GetString(memory.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task<byte> ReadByteAsync(CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(1);
        try
        {
            await Stream.ReadExactlyAsync(buffer.AsMemory(0, 1), token);
            return buffer[0];
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task<int> ReadInt32Async(CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(4);
        try
        {
            await Stream.ReadExactlyAsync(buffer.AsMemory(0, 4), token);
            return BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(0, 4));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task<bool> ReadBoolAsync(CancellationToken token)
    {
        var b = await ReadByteAsync(token);
        return b != 0;
    }

    public async Task<long> ReadInt64Async(CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(8);
        try
        {
            await Stream.ReadExactlyAsync(buffer.AsMemory(0, 8), token);
            return BinaryPrimitives.ReadInt64BigEndian(buffer.AsSpan(0, 8));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task<byte[]> ReadBufferAsync(CancellationToken token)
    {
        var length = await ReadInt32Async(token);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var memory = buffer.AsMemory(0, length);
            await Stream.ReadExactlyAsync(memory, token);
            return memory.ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task<string> ReadStringAsync(CancellationToken token)
    {
        var stringByteLength = await ReadInt32Async(token);
        if (stringByteLength == 0)
        {
            return string.Empty;
        }

        var buffer = ArrayPool<byte>.Shared.Rent(stringByteLength);
        try
        {
            var memory = buffer.AsMemory(0, stringByteLength);
            await Stream.ReadExactlyAsync(memory, token);
            return Encoding.UTF8.GetString(memory.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task<uint> ReadUInt32Async(CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(uint));
        try
        {
            await Stream.ReadExactlyAsync(buffer.AsMemory(0, sizeof(uint)), token);
            return BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(0, sizeof(uint)));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public ulong ReadUInt64()
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(ulong));
        try
        {
            var span = buffer.AsSpan(0, sizeof(ulong));
            Stream.ReadExactly(span);
            return BinaryPrimitives.ReadUInt64BigEndian(span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task<ulong> ReadUInt64Async(CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(uint));
        try
        {
            var memory = buffer.AsMemory(0, sizeof(ulong));
            await Stream.ReadExactlyAsync(memory, token);
            return BinaryPrimitives.ReadUInt64BigEndian(memory.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}