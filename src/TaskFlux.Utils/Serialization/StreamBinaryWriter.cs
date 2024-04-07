using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using TaskFlux.Core;

namespace TaskFlux.Utils.Serialization;

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

    public void Write(byte value)
    {
        Span<byte> buffer = stackalloc byte[1];
        buffer[0] = value;
        Stream.Write(buffer);
    }

    /// <summary>
    /// Записать буфер байтов в поток с указанием длины впереди.
    /// Длина указывается в формате Int32
    /// </summary>
    /// <param name="buffer">Буфер, который нужно записать</param>
    public void WriteBuffer(byte[] buffer)
    {
        // Длина
        Span<byte> span = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(span, buffer.Length);
        Stream.Write(span);

        // Данные
        Stream.Write(buffer);
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

    public void Write(bool value)
    {
        Span<byte> span = stackalloc byte[1];
        span[0] = ( byte ) ( value
                                 ? 1
                                 : 0 );

        Stream.Write(span);
    }

    public async Task WriteAsync(int value, CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(int));
        try
        {
            var memory = buffer.AsMemory(0, sizeof(int));
            BinaryPrimitives.WriteInt32BigEndian(memory.Span, value);
            await Stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task WriteAsync(QueueName queueName, CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(QueueNameParser.MaxNameLength + 1);
        buffer[0] = ( byte ) queueName.Name.Length;
        var temp = buffer.AsMemory(1, QueueNameParser.MaxNameLength);
        var length = QueueNameParser.Encoding.GetBytes(queueName.Name, temp.Span);
        await Stream.WriteAsync(buffer.AsMemory(0, length + 1), token);
    }

    public async Task WriteAsync(string value, CancellationToken token)
    {
        var stringLength = Encoding.UTF8.GetByteCount(value);
        var totalLength = stringLength + sizeof(int);
        var buffer = ArrayPool<byte>.Shared.Rent(totalLength);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(), stringLength);
            Encoding.UTF8.GetBytes(value, buffer.AsSpan(sizeof(int)));
            await Stream.WriteAsync(buffer.AsMemory(0, totalLength), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task WriteAsync(byte value, CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(1);
        try
        {
            buffer[0] = value;
            await Stream.WriteAsync(buffer.AsMemory(0, 1), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task WriteAsync(bool value, CancellationToken token)
    {
        await WriteAsync(( byte ) ( value
                                        ? 1
                                        : 0 ), token);
    }

    public async Task WriteAsync(long value, CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(long));
        try
        {
            var memory = buffer.AsMemory(0, sizeof(long));
            BinaryPrimitives.WriteInt64BigEndian(memory.Span, value);
            await Stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task WriteBufferAsync(byte[] data, CancellationToken token)
    {
        await WriteAsync(data.Length, token);
        await Stream.WriteAsync(data, token);
    }

    public async Task WriteAsync(ulong value, CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(ulong));
        try
        {
            var memory = buffer.AsMemory(0, sizeof(ulong));
            BinaryPrimitives.WriteUInt64BigEndian(memory.Span, value);
            await Stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public void Write(ulong value)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(ulong));
        try
        {
            var span = buffer.AsSpan(0, sizeof(ulong));
            BinaryPrimitives.WriteUInt64BigEndian(span, value);
            Stream.Write(span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}