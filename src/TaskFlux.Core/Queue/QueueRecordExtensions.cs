using TaskFlux.Domain;

namespace TaskFlux.Core.Queue;

public static class QueueRecordExtensions
{
    /// <summary>
    /// Получить <see cref="PriorityQueueData"/> из ID и нагрузки этой записи
    /// </summary>
    /// <returns>Данные этой записи</returns>
    public static PriorityQueueData GetPriorityQueueData(this QueueRecord record) => new(record.Id, record.Payload);

    public static QueueRecordData GetData(this QueueRecord record) => new(record.Priority, record.Payload);
}