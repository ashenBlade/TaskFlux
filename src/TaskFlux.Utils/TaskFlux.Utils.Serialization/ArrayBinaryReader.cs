using System.Buffers.Binary;
using System.Runtime.Serialization;
using System.Text;
using TaskFlux.Core;

namespace TaskFlux.Utils.Serialization;

public struct ArrayBinaryReader
{
    private readonly byte[] _buffer;
    private int _index = 0;

    public ArrayBinaryReader(byte[] buffer)
    {
        _buffer = buffer;
    }


    /// <summary>
    /// Прочитать <see cref="long"/> и сдвинуться на нужное кол-во байт
    /// </summary>
    /// <returns>Десериализованный <see cref="long"/></returns>
    /// <exception cref="SerializationException">В буфере нет нужного количества байт</exception>
    public byte ReadByte()
    {
        if (_buffer.Length <= _index)
        {
            throw new SerializationException("Нельзя прочитать байт. Место в буфере закончилось");
        }

        return _buffer[_index++];
    }


    /// <summary>
    /// Прочитать <see cref="int"/> и сдвинуться на нужное кол-во байт
    /// </summary>
    /// <returns>Десериализованный <see cref="int"/></returns>
    /// <exception cref="SerializationException">В буфере нет нужного количества байт</exception>
    public int ReadInt32()
    {
        try
        {
            var value = BinaryPrimitives.ReadInt32BigEndian(_buffer.AsSpan(_index));
            _index += sizeof(int);
            return value;
        }
        catch (ArgumentOutOfRangeException e)
        {
            throw new SerializationException("Ошибка десериализации int: в буфере не осталось нужного количества байт",
                e);
        }
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
            var value = BinaryPrimitives.ReadInt64BigEndian(_buffer.AsSpan(_index));
            _index += sizeof(long);
            return value;
        }
        catch (ArgumentOutOfRangeException e)
        {
            throw new SerializationException("Ошибка десериализации long: в буфере не осталось нужного количества байт",
                e);
        }
    }

    /// <summary>
    /// Проверить, что в буфере есть <paramref name="shouldHasLength"/> свободных байтов
    /// </summary>
    /// <param name="shouldHasLength">Количество свободных байтов</param>
    /// <exception cref="SerializationException">В буфере нет требуемого количества свободных байт</exception>
    private void EnsureLength(int shouldHasLength)
    {
        var leftLength = _buffer.Length - _index;
        if (leftLength < shouldHasLength)
        {
            throw new SerializationException(
                $"В буфере отсутствует указанное количество байт. Требуется: {shouldHasLength}. Оставшееся количество: {leftLength}");
        }
    }

    /// <summary>
    /// Прочитать сериализованный массив из буфера и сдвинуть позицию на прочитанное кол-во данных
    /// </summary>
    /// <returns>Сериализованный массив</returns>
    /// <exception cref="OverflowException">Прочитанный размер буфера меньше 0</exception>
    /// <exception cref="SerializationException">Оставшееся кол-во байтов меньше требуемого для буфера</exception>
    public byte[] ReadBuffer()
    {
        var length = ReadInt32();
        if (length == 0)
        {
            return Array.Empty<byte>();
        }

        EnsureLength(length);
        var span = _buffer.AsSpan(_index, length);
        try
        {
            var buffer = new byte[length];
            span.CopyTo(buffer);
            _index += length;
            return buffer;
        }
        catch (ArgumentOutOfRangeException e)
        {
            throw new SerializationException(
                "Ошибка десериализации массива: в буфере не осталось нужного количества байт", e);
        }
    }

    /// <summary>
    /// Десериализовать строку из буфера и сдвинуть позицию на нужное количество байт
    /// </summary>
    /// <remarks>Используется UTF-8 кодировка</remarks>
    /// <returns>Десериализованная строка</returns>
    /// <exception cref="SerializationException">Ошибка во время десерилазации</exception>
    public string ReadString()
    {
        var length = ReadInt32();
        Span<byte> stringSpan;
        try
        {
            stringSpan = _buffer.AsSpan(_index, length);
        }
        catch (ArgumentOutOfRangeException e)
        {
            throw new SerializationException(
                $"Ошибка десериализации строки: в буффере нет нужного количества байт: требуется {length}", e);
        }

        // Надо правильно обрабатываеть неправильную последовательность байт
        // почему-то UTF8Encoding класс не кидает исключения при ошибках и возвращает мусор

        var str = Encoding.UTF8.GetString(stringSpan);
        _index += length;
        return str;
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
    /// Десериализовать <see cref="uint"/> из буфера
    /// </summary>
    /// <returns>Десериализованный <see cref="uint"/></returns>
    /// <exception cref="SerializationException">В буфере нет 4 байт для десериализации числа</exception>
    public uint ReadUInt32()
    {
        try
        {
            var value = BinaryPrimitives.ReadUInt32BigEndian(_buffer.AsSpan(_index));
            _index += sizeof(uint);
            return value;
        }
        catch (ArgumentOutOfRangeException e)
        {
            throw new SerializationException("Ошибка во время десериализации uint: в буфере нет места", e);
        }
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
        var span = _buffer.AsSpan(++_index, length);
        var name = QueueNameParser.Parse(span);
        _index += length;
        return name;
    }
}