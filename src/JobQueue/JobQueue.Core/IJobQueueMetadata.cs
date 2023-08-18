namespace JobQueue.Core;

public interface IJobQueueMetadata
{
    /// <summary>
    /// Название очереди
    /// </summary>
    public QueueName QueueName { get; }
    
    /// <summary>
    /// Текущий размер очереди (количество элементов в ней)
    /// </summary>
    public uint Count { get; }
    
    /// <summary>
    /// Максимальный размер очереди
    /// </summary>
    /// <remarks>0 означает отсутствие предела</remarks>
    public uint MaxSize { get; }
    
    /// <summary>
    /// Имеет ли очередь предел количества элементов
    /// </summary>
    public bool HasMaxSize => MaxSize > 0;
}