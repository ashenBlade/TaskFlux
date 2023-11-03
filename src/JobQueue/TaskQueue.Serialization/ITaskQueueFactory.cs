using TaskQueue.Core;

namespace TaskQueue.Serialization;

public interface ITaskQueueFactory
{
    /// <summary>
    /// Создать очередь задач используя переданные стандартные аргументы.
    /// </summary>
    /// <param name="name">Название очереди</param>
    /// <param name="maxSize">Максимальный размер очереди. 0 - без предела</param>
    /// <param name="data">
    /// Данные, которые должны храниться в очереди
    /// </param>
    /// <returns></returns>
    public ITaskQueue CreateTaskQueue(QueueName name,
                                      uint maxSize,
                                      IReadOnlyCollection<(long Priority, byte[] Payload)> data);
}