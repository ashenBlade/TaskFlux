using TaskFlux.Models;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Core.Queue;

public interface IReadOnlyTaskQueue
{
    /// <summary>
    /// Тип реализации приоритетной очереди
    /// </summary>
    public PriorityQueueCode Code { get; }

    /// <summary>
    /// Название текущей очереди
    /// </summary>
    public QueueName Name { get; }

    /// <summary>
    /// Количество элементов в очереди
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// Метаданные очереди
    /// </summary>
    public ITaskQueueMetadata Metadata { get; }

    /// <summary>
    /// Получить список хранящихся в очереди данных в виде пары Приоритет/Нагрузка
    /// </summary>
    /// <returns>Набор хранящихся в очереди данных</returns>
    /// <remarks>
    /// Метод предназначен для сериализации.
    /// Список возвращаемых данных не обязан быть в правильном порядке
    /// </remarks>
    public IReadOnlyCollection<(long Priority, byte[] Payload)> ReadAllData();
}