using System.Buffers.Binary;
using System.Text;

namespace TaskFlux.Serialization.Helpers;

public ref struct SpanBinaryReader
{
    private readonly Span<byte> _buffer = Span<byte>.Empty;
    private int _index = 0;

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
}