using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;

namespace TaskFlux.Utils.Serialization;

public ref struct SpanBinaryWriter
{
    public Span<byte> Buffer = Span<byte>.Empty;

    private int _index = 0;

    /// <summary>
    /// Текущий индекс начала записи в <see cref="Buffer"/>
    /// </summary>
    public int Index => _index;

    public SpanBinaryWriter(Span<byte> buffer)
    {
        Buffer = buffer;
    }

    public void Write(byte value)
    {
        Buffer[_index] = value;
        _index++;
    }

    public void Write(int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(Buffer[_index..], value);
        _index += sizeof(int);
    }

    /// <summary>
    /// Записать в буфер переданный массив байт как есть (т.е. без маркера длины)
    /// </summary>
    /// <param name="span">Байты, которые нужно записать</param>
    public void Write(ReadOnlySpan<byte> span)
    {
        span.CopyTo(Buffer[_index..]);
        _index += span.Length;
    }

    /// <summary>
    /// Сериализовать переданный буфер как самостоятельную единицу.
    /// Сериализуется как длина массива, так и сами значения
    /// </summary>
    /// <param name="buffer">Буфер для сериализации</param>
    public void WriteBuffer(ReadOnlySpan<byte> buffer)
    {
        BinaryPrimitives.WriteInt32BigEndian(Buffer[_index..], buffer.Length);
        _index += sizeof(int);
        buffer.CopyTo(Buffer[_index..]);
        _index += buffer.Length;
    }

    /// <summary>
    /// Сериализовать переданный буфер как самостоятельную единицу с учетом выравнивания.
    /// Сериализуются длина массива, сам массив данных и выравнивание.
    /// Байты выравнивания заполняются 0
    /// </summary>
    /// <param name="buffer">Буфер для сериализации</param>
    /// <param name="alignment">По какой границе необходимо делать выравнивание</param>
    public void WriteBufferAligned(ReadOnlySpan<byte> buffer, int alignment)
    {
        Debug.Assert(0 <= alignment, "0 <= alignment", "Выравнивание не может быть отрицательным");
        BinaryPrimitives.WriteInt32BigEndian(Buffer[_index..], buffer.Length);
        _index += sizeof(int);
        buffer.CopyTo(Buffer[_index..]);
        _index += buffer.Length;

        // Дальше идет выравнивание
        if (alignment == 0)
        {
            return;
        }

        var align = buffer.Length % alignment;
        if (align == 0)
        {
            return;
        }

        Span<byte> span = stackalloc byte[align];
        span.Clear();
        span.CopyTo(Buffer[_index..]);
        _index += align;
    }

    /// <summary>
    /// Сериализовать переданный массив в буфер
    /// </summary>
    /// <param name="value"></param>
    public void Write(byte[] value)
    {
        value.CopyTo(Buffer[_index..]);
        _index += value.Length;
    }

    /// <summary>
    /// Записать в буфер переданную строку
    /// </summary>
    /// <param name="value">Строка для сериализации</param>
    public void Write(string value)
    {
        var stringByteLength = Encoding.UTF8.GetBytes(value, Buffer[( _index + sizeof(int) )..]);
        BinaryPrimitives.WriteInt32BigEndian(Buffer[_index..], stringByteLength);
        _index += sizeof(int) + stringByteLength;
    }

    public void Write(bool resultSuccess)
    {
        const byte byteTrue = 1;
        const byte byteFalse = 0;
        Buffer[_index] = resultSuccess
                             ? byteTrue
                             : byteFalse;
        _index++;
    }

    public void Write(uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(Buffer[_index..], value);
        _index += sizeof(uint);
    }

    public void Write(long value)
    {
        BinaryPrimitives.WriteInt64BigEndian(Buffer[_index..], value);
        _index += sizeof(long);
    }
}