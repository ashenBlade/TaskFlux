using Utils.Serialization;

namespace TaskFlux.Delta;

/// <summary>
/// Абстрактный класс, представляющий дельту - изменение в состоянии приложения
/// </summary>
public abstract class Delta
{
    /// <summary>
    /// Тип дельты, которую представляет этот объект
    /// </summary>
    public abstract DeltaType Type { get; }

    /// <summary>
    /// Сериализовать дельту в массив байт
    /// </summary>
    public abstract byte[] Serialize();

    internal Delta()
    {
    }

    /// <summary>
    /// Десериализовать объект дельты из переданного потока
    /// </summary>
    /// <param name="stream">Поток, из которого нужно десериализовать дельту</param>
    /// <returns>Прочитанная дельта</returns>
    /// <exception cref="EndOfStreamException"><paramref name="stream"/> был пуст, либо в нем оказалось недостаточно данных для десерилазации</exception>
    /// <exception cref="UnknownDeltaTypeException">Прочитанный байт-маркер дельты неизвестен</exception>
    public static Delta DeserializeFrom(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[1];
        stream.ReadExactly(buffer);
        var marker = buffer[0];
        switch (( DeltaType ) marker)
        {
            case DeltaType.CreateQueue:
                return DeserializeCreateQueue(stream);
            case DeltaType.DeleteQueue:
                return DeserializeDeleteQueue(stream);
            case DeltaType.AddRecord:
                return DeserializeAddRecord(stream);
            case DeltaType.RemoveRecord:
                return DeserializeRemoveRecord(stream);
        }

        throw new UnknownDeltaTypeException(marker);
    }

    private static RemoveRecordDelta DeserializeRemoveRecord(Stream stream)
    {
        var reader = new StreamBinaryReader(stream);
        var queueName = reader.ReadQueueName();
        var key = reader.ReadInt64();
        var message = reader.ReadBuffer();
        return new RemoveRecordDelta(queueName, key, message);
    }

    private static AddRecordDelta DeserializeAddRecord(Stream stream)
    {
        var reader = new StreamBinaryReader(stream);
        var queueName = reader.ReadQueueName();
        var key = reader.ReadInt64();
        var message = reader.ReadBuffer();
        return new AddRecordDelta(queueName, key, message);
    }

    private static DeleteQueueDelta DeserializeDeleteQueue(Stream stream)
    {
        var reader = new StreamBinaryReader(stream);
        return new DeleteQueueDelta(reader.ReadQueueName());
    }

    private static CreateQueueDelta DeserializeCreateQueue(Stream stream)
    {
        var reader = new StreamBinaryReader(stream);
        var queueName = reader.ReadQueueName();
        var implementation = reader.ReadInt32();
        var maxQueueSize = reader.ReadInt32();
        var maxMessageSize = reader.ReadInt32();
        (long, long)? priorityRange = null;
        if (reader.ReadBool())
        {
            priorityRange = ( reader.ReadInt64(), reader.ReadInt64() );
        }

        return new CreateQueueDelta(queueName, implementation, maxQueueSize, maxMessageSize, priorityRange);
    }
}