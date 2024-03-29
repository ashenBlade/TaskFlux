using TaskFlux.Core.Queue;

namespace TaskFlux.Core.Waiter;

/// <summary>
/// Менеджер для управления подписчиками очереди
/// </summary>
public interface IQueueSubscriberManager
{
    /// <summary>
    /// Уведомить подписчиков о новой записи.
    /// Если кто-то был уведомлен, то эта запись отправляется нужному подписчику
    /// </summary>
    /// <param name="record">Запись, о которой нужно уведомить</param>
    /// <returns><c>true</c> - подписчик уведомлен и запись успешна передана ему, <c>false</c> - никто новых записей не ждет</returns>
    public bool TryNotifyRecord(QueueRecord record);

    /// <summary>
    /// Создать очередного подписчика, которому будут направляться новые записи 
    /// </summary>
    /// <returns>Подписчик очереди</returns>
    public IQueueSubscriber GetSubscriber();
}