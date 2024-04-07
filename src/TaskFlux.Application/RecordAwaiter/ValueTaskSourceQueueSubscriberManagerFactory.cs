using Microsoft.Extensions.ObjectPool;
using TaskFlux.Core.Subscription;

namespace TaskFlux.Application.RecordAwaiter;

public class ValueTaskSourceQueueSubscriberManagerFactory : IQueueSubscriberManagerFactory
{
    private readonly ObjectPool<ValueTaskSourceQueueSubscriberManager.QueueSubscriber> _pool;

    /// <summary>
    /// Создать новую фабрику для менеджеров подписчиков
    /// </summary>
    /// <param name="retainCount">Максимальное количество объектов, которое можно хранить в пуле</param>
    public ValueTaskSourceQueueSubscriberManagerFactory(int retainCount)
    {
        _pool = new DefaultObjectPool<ValueTaskSourceQueueSubscriberManager.QueueSubscriber>(
            new RecordAwaiterPooledObjectPolicy(), retainCount);
    }

    public IQueueSubscriberManager CreateQueueSubscriberManager()
    {
        return new ValueTaskSourceQueueSubscriberManager(_pool);
    }

    /// <summary>
    /// Политика создания объектов для <see cref="ValueTaskSourceQueueSubscriberManager.QueueSubscriber"/>, т.к. стандартная реализация использует рефлексию
    /// </summary>
    private class
        RecordAwaiterPooledObjectPolicy : IPooledObjectPolicy<ValueTaskSourceQueueSubscriberManager.QueueSubscriber>
    {
        public ValueTaskSourceQueueSubscriberManager.QueueSubscriber Create()
        {
            return new ValueTaskSourceQueueSubscriberManager.QueueSubscriber();
        }

        public bool Return(ValueTaskSourceQueueSubscriberManager.QueueSubscriber obj)
        {
            return true;
        }
    }
}