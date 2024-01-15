using TaskFlux.PriorityQueue;

namespace TaskFlux.Core.Queue;

public interface ITaskQueueMetadata
{
    /// <summary>
    /// Название очереди
    /// </summary>
    public QueueName QueueName { get; }

    /// <summary>
    /// Код реализации очереди
    /// </summary>
    public PriorityQueueCode Code { get; }

    /// <summary>
    /// Текущий размер очереди (количество элементов в ней)
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// Максимальный размер очереди
    /// </summary>
    public int? MaxQueueSize { get; }

    /// <summary>
    /// Имеет ли очередь предел количества элементов
    /// </summary>
    public bool HasMaxSize => MaxQueueSize is not null;

    public int? MaxPayloadSize { get; }

    public (long Min, long Max)? PriorityRange { get; }
}