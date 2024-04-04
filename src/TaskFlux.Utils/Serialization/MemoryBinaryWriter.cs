using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Text;
using TaskFlux.Core;

namespace TaskFlux.Utils.Serialization;

public struct MemoryBinaryWriter
{
    private Memory<byte> _buffer;
    private int _index = 0;

    public MemoryBinaryWriter(Memory<byte> buffer)
    {
        _buffer = buffer;
    }

    public void Write(byte value)
    {
        _buffer.Span[_index] = value;
        _index++;
    }

    public int Write(int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(_buffer[_index..].Span, value);
        _index += sizeof(int);
        return sizeof(int);
    }

    public void Write(long value)
    {
        BinaryPrimitives.WriteInt64BigEndian(_buffer[_index..].Span, value);
        _index += sizeof(long);
    }

    public void Write(uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(_buffer[_index..].Span, value);
        _index += sizeof(uint);
    }

    /// <summary>
    /// Сериализовать переданный буфер как самостоятельную единицу.
    /// Сериализуется как длина массива, так и сами значения
    /// </summary>
    /// <param name="buffer">Буфер для сериализации</param>
    public int WriteBuffer(byte[] buffer)
    {
        BinaryPrimitives.WriteInt32BigEndian(_buffer.Slice(_index).Span, buffer.Length);
        _index += sizeof(int);
        buffer.CopyTo(_buffer.Slice(_index).Span);
        _index += buffer.Length;
        return sizeof(int) + buffer.Length;
    }

    /// <summary>
    /// Сериализовать переданный массив в буфер
    /// </summary>
    /// <param name="value"></param>
    public void Write(byte[] value)
    {
        value.CopyTo(_buffer.Slice(_index).Span);
        _index += value.Length;
    }

    /// <summary>
    /// Записать в буфер переданную строку
    /// </summary>
    /// <param name="value">Строка для сериализации</param>
    /// <exception cref="SerializationException">В буфере недостаточно места для серилазиации строки</exception>
    public void Write(string value)
    {
        var stringLength = Encoding.UTF8.GetByteCount(value);
        EnsureLength(sizeof(int) + stringLength);

        var stringByteLength = Encoding.UTF8.GetBytes(value, _buffer.Slice(_index + sizeof(int)).Span);
        BinaryPrimitives.WriteInt32BigEndian(_buffer[_index..].Span, stringByteLength);
        _index += sizeof(int) + stringByteLength;
    }


    /// <summary>
    /// Сериализовать название очереди в буфер и сдвинуть позицию на нужное количество байт
    /// </summary>
    /// <param name="name">Название очереди</param>
    /// <exception cref="SerializationException">Ошибка во время сериализации названия (например, недостаточно места в буфере)</exception>
    public void Write(QueueName name)
    {
        if (name.IsDefaultQueue /* "" */)
        {
            EnsureLength(1);
            // Наверное, часто будет использоваться название по умолчанию (пустая строка)
            _buffer.Span[_index++] = 0;
            return;
        }

        // Длина точно в диапазоне [1; 255]
        var length = ( byte ) name.Name.Length;
        EnsureLength(length + 1);
        _buffer.Span[_index++] = length;
        var written = QueueNameParser.Encoding.GetBytes(name.Name, _buffer[_index..].Span);
        Debug.Assert(written == length, "Записанная длина должна быть равна вычисленной");
        _index += length;
    }

    /// <summary>
    /// Записать строку, как название очереди (размер 1 байт, ASCII и т.д.)
    /// </summary>
    /// <param name="queueName">Сырое название очереди</param>
    public void WriteAsQueueName(string queueName)
    {
        Debug.Assert(queueName.Length < byte.MaxValue, "queueName.Length < byte.MaxValue",
            "Название очереди не может превышать 1 байта");
        var stringByteLength = Encoding.ASCII.GetByteCount(queueName);
        EnsureLength(sizeof(byte) + stringByteLength);
        if (stringByteLength < 127)
        {
            Span<byte> stringBytes = stackalloc byte[stringByteLength + 1];
            stringBytes[0] = ( byte ) queueName.Length;
            Encoding.ASCII.GetBytes(queueName, stringBytes[1..]);
            stringBytes.CopyTo(_buffer.Span[_index..]);
        }
        else
        {
            var buffer = ArrayPool<byte>.Shared.Rent(stringByteLength + 1);
            try
            {
                buffer[0] = ( byte ) queueName.Length;
                var written = Encoding.ASCII.GetBytes(queueName, buffer.AsSpan(1));
                buffer.AsSpan(0, 1 + written).CopyTo(_buffer.Span[_index..]);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        _index += 1 + stringByteLength;
    }

    /// <summary>
    /// Проверить, что в буфере еще имеется <paramref name="shouldHasLength"/> свободных байт места
    /// </summary>
    /// <param name="shouldHasLength">Количество свободных байт для записи</param>
    /// <exception cref="SerializationException">В буфере недостаточно места</exception>
    private void EnsureLength(int shouldHasLength)
    {
        var left = _buffer.Length - _index;
        if (left < shouldHasLength)
        {
            throw new SerializationException(
                $"В буфере недостаточно места для записи. Требуется {shouldHasLength}. Осталось: {left}");
        }
    }

    public static int EstimateResultSize(string value) => sizeof(int)                        // Размер 
                                                        + Encoding.UTF8.GetByteCount(value); // Строка

    public static int EstimateResultSize(QueueName name) => sizeof(byte)                            // Размер
                                                          + Encoding.ASCII.GetByteCount(name.Name); // Длина

    public void Write(bool resultSuccess)
    {
        const byte byteTrue = 1;
        const byte byteFalse = 0;

        _buffer.Span[_index] = resultSuccess
                                   ? byteTrue
                                   : byteFalse;
        _index++;
    }

    public static int EstimateResultSizeAsQueueName(string name) => sizeof(byte)                       // Размер
                                                                  + Encoding.ASCII.GetByteCount(name); // Длина 

    public void Write(ulong value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        buffer.CopyTo(_buffer.Span[_index..]);
        _index += sizeof(ulong);
    }
}