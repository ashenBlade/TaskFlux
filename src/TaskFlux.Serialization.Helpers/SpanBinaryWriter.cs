using System.Buffers.Binary;
using System.Text;

namespace TaskFlux.Serialization.Helpers;

public ref struct SpanBinaryWriter
{
    public Span<byte> Buffer = Span<byte>.Empty;
    private int _index = 0;

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
}