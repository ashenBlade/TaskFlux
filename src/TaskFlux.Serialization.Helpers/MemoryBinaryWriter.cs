using System.Buffers.Binary;
using System.Text;

namespace TaskFlux.Serialization.Helpers;

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

    public void Write(int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(_buffer[_index..].Span, value);
        _index += sizeof(int);
    }
    
    public void Write(long value)
    {
        BinaryPrimitives.WriteInt64BigEndian(_buffer[_index..].Span, value);
        _index += sizeof(long);
    }

    /// <summary>
    /// Сериализовать переданный буфер как самостоятельную единицу.
    /// Сериализуется как длина массива, так и сами значения
    /// </summary>
    /// <param name="buffer">Буфер для сериализации</param>
    public void WriteBuffer(byte[] buffer)
    {
        BinaryPrimitives.WriteInt32BigEndian(_buffer.Slice(_index).Span, buffer.Length);
        _index+= sizeof(int);
        buffer.CopyTo(_buffer.Slice(_index).Span);
        _index += buffer.Length;
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
    public void Write(string value)
    {
        var stringByteLength = Encoding.UTF8.GetBytes(value, _buffer.Slice(_index + sizeof(int)).Span);
        BinaryPrimitives.WriteInt32BigEndian(_buffer.Slice(_index).Span, stringByteLength);
        _index += sizeof(int) + stringByteLength;
    }

    public static int EstimateResultSize(string value) => sizeof(int)                        // Размер 
                                                       + Encoding.UTF8.GetByteCount(value); // Строка

    public void Write(bool resultSuccess)
    {
        const byte byteTrue = 1;
        const byte byteFalse = 0;
        
        _buffer.Span[_index] = resultSuccess
                                   ? byteTrue
                                   : byteFalse;
        _index++;
    }
}