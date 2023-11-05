using TaskFlux.Models;

namespace TaskFlux.Abstractions;

public interface ITaskQueueMetadata
{
    /// <summary>
    /// Название очереди
    /// </summary>
    public QueueName QueueName { get; }

    /// <summary>
    /// Текущий размер очереди (количество элементов в ней)
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// Максимальный размер очереди
    /// </summary>
    public int? MaxSize { get; }

    /// <summary>
    /// Имеет ли очередь предел количества элементов
    /// </summary>
    public bool HasMaxSize => MaxSize is not null;

    public int? MaxPayloadSize { get; }

    public (long Min, long Max)? PriorityRange { get; }
}