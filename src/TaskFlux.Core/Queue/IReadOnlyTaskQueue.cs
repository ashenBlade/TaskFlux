using TaskFlux.Core.Policies;
using TaskFlux.Domain;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Core.Queue;

/// <summary>
/// Интерфейс очереди задач только для чтения
/// </summary>
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
    /// Политики этой очереди
    /// </summary>
    public IReadOnlyList<QueuePolicy> Policies { get; }

    /// <summary>
    /// Метаданные очереди
    /// </summary>
    public ITaskQueueMetadata Metadata { get; }

    /// <summary>
    /// Последний назначенный ID записи
    /// </summary>
    public RecordId LastId { get; }

    /// <summary>
    /// Получить список хранящихся в очереди данных в виде пары Приоритет/Нагрузка
    /// </summary>
    /// <returns>Набор хранящихся в очереди данных</returns>
    /// <remarks>
    /// Метод предназначен для сериализации.
    /// Список возвращаемых данных не обязан быть в правильном порядке
    /// </remarks>
    public IReadOnlyCollection<QueueRecord> ReadAllData();
}