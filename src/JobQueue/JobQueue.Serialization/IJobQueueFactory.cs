using JobQueue.Core;

namespace JobQueue.Serialization;

public interface IJobQueueFactory
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
    public IJobQueue CreateJobQueue(QueueName name,
                                    uint maxSize,
                                    IReadOnlyCollection<(long Priority, byte[] Payload)> data);
}