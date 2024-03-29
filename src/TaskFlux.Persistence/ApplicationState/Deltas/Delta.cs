using TaskFlux.Core.Restore;

namespace TaskFlux.Persistence.ApplicationState.Deltas;

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
                return CreateQueueDelta.Deserialize(buffer);
            case DeltaType.DeleteQueue:
                return DeleteQueueDelta.Deserialize(buffer);
            case DeltaType.AddRecord:
                return AddRecordDelta.Deserialize(buffer);
            case DeltaType.RemoveRecord:
                return RemoveRecordDelta.Deserialize(buffer);
        }

        throw new UnknownDeltaTypeException(marker);
    }
}