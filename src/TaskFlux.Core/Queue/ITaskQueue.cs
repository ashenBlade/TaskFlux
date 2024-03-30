using TaskFlux.Core.Subscription;

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
    /// <c>true</c> - значение было добавлено в очередь, <br/>
    /// <c>false</c> - в очереди нет места для новых элементов
    /// </returns>
    public EnqueueResult Enqueue(long priority, byte[] payload);

    /// <summary>
    /// Получить элемент из очереди
    /// </summary>
    /// <param name="record">Прочитанная запись</param>
    /// <returns><c>true</c> - элемент получен, <c>false</c> - очередь была пуста и ничего не получено</returns>
    public bool TryDequeue(out QueueRecord record);

    /// <summary>
    /// Получить ожидателя новой записи в очереди
    /// </summary>
    public IQueueSubscriber Subscribe();
}