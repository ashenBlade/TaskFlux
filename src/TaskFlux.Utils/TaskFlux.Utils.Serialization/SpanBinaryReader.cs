using System.Buffers.Binary;
using System.Runtime.Serialization;
using System.Text;
using TaskFlux.Core;

namespace TaskFlux.Utils.Serialization;

public ref struct SpanBinaryReader
{
    private readonly Span<byte> _buffer = Span<byte>.Empty;
    private int _index = 0;

    /// <summary>
    /// Позиция, с которой начинается чтение данных
    /// </summary>
    public int Index => _index;

    public SpanBinaryReader(Span<byte> buffer)
    {
        _buffer = buffer;
    }

    public byte ReadByte()
    {
        return _buffer[_index++];
    }

    public int ReadInt32()
    {
        var value = BinaryPrimitives.ReadInt32BigEndian(_buffer[_index..]);
        _index += sizeof(int);
        return value;
    }

    private void EnsureLength(int shouldHasLength)
    {
        var leftLength = _buffer.Length - _index;
        if (leftLength < shouldHasLength)
        {
            throw new InvalidDataException(
                $"В буфере отсутствует указанное количество байт. Требуется: {shouldHasLength}. Оставшееся количество: {leftLength}");
        }
    }

    public byte[] ReadBuffer()
    {
        var length = ReadInt32();
        EnsureLength(length);
        var buffer = new byte[length];
        _buffer.Slice(_index, length).CopyTo(buffer);
        _index += length;
        return buffer;
    }

    public byte[] ReadBufferAligned(int alignment)
    {
        var length = ReadInt32();
        var align = GetAlign(length, alignment);
        EnsureLength(length + align);
        var buffer = new byte[length];
        _buffer.Slice(_index, length).CopyTo(buffer);
        _index += length + align;
        return buffer;
    }

    private static int GetAlign(int length, int alignment)
    {
        return alignment == 0
            ? 0
            : length % alignment;
    }

    public string ReadString()
    {
        var length = ReadInt32();
        var str = Encoding.UTF8.GetString(_buffer.Slice(_index, length));
        _index += length;
        return str;
    }

    public bool ReadBoolean()
    {
        var value = _buffer[_index];
        _index++;
        return value != 0;
    }

    public uint ReadUInt32()
    {
        var stored = BinaryPrimitives.ReadUInt32BigEndian(_buffer.Slice(_index, 4));
        _index += sizeof(uint);
        return stored;
    }

    /// <summary>
    /// Прочитать из буфера сериализованное название очереди
    /// </summary>
    /// <returns>Десериализованное название очереди</returns>
    /// <exception cref="InvalidQueueNameException">Хранившееся название очереди было в неверном формате</exception>
    /// <exception cref="SerializationException">Во время десериализации возникло исключение</exception>
    public QueueName ReadQueueName()
    {
        if (_buffer.Length <= _index)
        {
            throw new SerializationException("Невозможно прочитать название очереди: в буфере не осталось места");
        }

        var length = _buffer[_index];
        EnsureLength(length);
        var span = _buffer.Slice(++_index, length);
        var name = QueueNameParser.Parse(span);
        _index += length;
        return name;
    }

    /// <summary>
    /// Прочитать закодированный <see cref="bool"/> и сдвинуть позицию на нужное количество байт
    /// </summary>
    /// <remarks>0 - это <c>false</c>, остальное - <c>true</c></remarks>
    /// <returns>Десериализованный <see cref="bool"/></returns>
    /// <exception cref="SerializationException">В буфере не осталось места для десериализации</exception>
    public bool ReadBool()
    {
        const byte falseByte = 0;

        if (_buffer.Length <= _index)
        {
            throw new SerializationException("Ошибка десериализации bool: в буфере закончилось место");
        }

        var value = _buffer[_index];
        _index++;
        return value != falseByte;
    }

    /// <summary>
    /// Прочитать <see cref="long"/> и сдвинуться на нужное кол-во байт
    /// </summary>
    /// <returns>Десериализованный <see cref="long"/></returns>
    /// <exception cref="SerializationException">В буфере нет нужного количества байт</exception>
    public long ReadInt64()
    {
        try
        {
            var value = BinaryPrimitives.ReadInt64BigEndian(_buffer.Slice(_index));
            _index += sizeof(long);
            return value;
        }
        catch (ArgumentOutOfRangeException e)
        {
            throw new SerializationException("Ошибка десериализации long: в буфере не осталось нужного количества байт",
                e);
        }
    }

    public ulong ReadUInt64()
    {
        try
        {
            var value = BinaryPrimitives.ReadUInt64BigEndian(_buffer.Slice(_index));
            _index += sizeof(ulong);
            return value;
        }
        catch (ArgumentOutOfRangeException e)
        {
            throw new SerializationException(
                "Ошибка десериализации ulong: в буфере не осталось нужного количества байт", e);
        }
    }
}