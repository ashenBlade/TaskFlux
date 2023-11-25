using TaskFlux.PriorityQueue;
using Utils.Serialization;

namespace TaskFlux.Serialization;

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
    /// Применить дельту к указанному множеству очередей
    /// </summary>
    /// <param name="queues">Множество очередей, над которым нужно применить операцию</param>
    public abstract void Apply(QueueCollection queues);

    /// <summary>
    /// Десериализовать объект дельты из переданного потока
    /// </summary>
    /// <param name="buffer">Поток, из которого нужно десериализовать дельту</param>
    /// <returns>Прочитанная дельта</returns>
    /// <exception cref="UnknownDeltaTypeException">Прочитанный байт-маркер дельты неизвестен</exception>
    public static Delta DeserializeFrom(byte[] buffer)
    {
        var marker = buffer[0];
        switch (( DeltaType ) marker)
        {
            case DeltaType.CreateQueue:
                return DeserializeCreateQueue(buffer);
            case DeltaType.DeleteQueue:
                return DeserializeDeleteQueue(buffer);
            case DeltaType.AddRecord:
                return DeserializeAddRecord(buffer);
            case DeltaType.RemoveRecord:
                return DeserializeRemoveRecord(buffer);
        }

        throw new UnknownDeltaTypeException(marker);
    }

    private static RemoveRecordDelta DeserializeRemoveRecord(byte[] buffer)
    {
        var reader = new SpanBinaryReader(buffer.AsSpan(1));
        var queueName = reader.ReadQueueName();
        var key = reader.ReadInt64();
        var message = reader.ReadBuffer();
        return new RemoveRecordDelta(queueName, key, message);
    }

    private static AddRecordDelta DeserializeAddRecord(byte[] buffer)
    {
        var reader = new SpanBinaryReader(buffer.AsSpan(1));
        var queueName = reader.ReadQueueName();
        var key = reader.ReadInt64();
        var message = reader.ReadBuffer();
        return new AddRecordDelta(queueName, key, message);
    }

    private static DeleteQueueDelta DeserializeDeleteQueue(byte[] buffer)
    {
        var reader = new SpanBinaryReader(buffer.AsSpan(1));
        return new DeleteQueueDelta(reader.ReadQueueName());
    }

    private static CreateQueueDelta DeserializeCreateQueue(byte[] buffer)
    {
        var reader = new SpanBinaryReader(buffer.AsSpan(1));
        var queueName = reader.ReadQueueName();
        var implementation = ( PriorityQueueCode ) reader.ReadInt32();
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