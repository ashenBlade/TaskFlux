namespace TaskFlux.Core.Waiter;

/// <summary>
/// Фабрика для создания новых менеджеров подписчиков очередей
/// </summary>
public interface IQueueSubscriberManagerFactory
{
    /// <summary>
    /// Создать нового менеджера подписчиков.
    /// Используется при создании новых очередей
    /// </summary>
    /// <returns>Новый менеджер подписчиков</returns>
    public IQueueSubscriberManager CreateQueueSubscriberManager();
}