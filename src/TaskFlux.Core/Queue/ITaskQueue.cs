using TaskFlux.Core.Subscription;
using TaskFlux.Domain;

namespace TaskFlux.Core.Queue;

/// <summary>
/// Интерфейс очереди задач
/// </summary>
public interface ITaskQueue : IReadOnlyTaskQueue
{
    /// <summary>
    /// Добавить новый элемент в очередь
    /// </summary>
    /// <param name="priority">Ключ</param>
    /// <param name="payload">Данные</param>
    /// <returns>
    /// Новая добавленная запись
    /// </returns>
    public QueueRecord Enqueue(long priority, byte[] payload);

    /// <summary>
    /// Добавить в очередь существующую запись
    /// </summary>
    /// <param name="record">Существующая запись</param>
    public void EnqueueExisting(QueueRecord record);

    /// <summary>
    /// Получить элемент из очереди
    /// </summary>
    /// <param name="record">Прочитанная запись</param>
    /// <returns><c>true</c> - элемент получен, <c>false</c> - очередь была пуста и ничего не получено</returns>
    public bool TryDequeue(out QueueRecord record);

    /// <summary>
    /// Получить подписчика новой записи для очереди
    /// </summary>
    public IQueueSubscriber Subscribe();
}