using System.Diagnostics;
using TaskFlux.Core.Restore;
using TaskFlux.Core.Subscription;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Core.Queue;

public class TaskQueueManager : ITaskQueueManager
{
    /// <summary>
    /// Фабрика для создания новых менеджеров ждунов для очередей
    /// </summary>
    private readonly IQueueSubscriberManagerFactory _queueSubscriberManagerFactory;

    /// <summary>
    /// Очереди, которыми мы управляем
    /// </summary>
    private readonly Dictionary<QueueName, ITaskQueue> _queues;

    public int QueuesCount => _queues.Count;

    /// <summary>
    /// Конструктор изначального менеджера
    /// </summary>
    /// <param name="defaultTaskQueue">Очередь по умолчанию</param>
    /// <param name="queueSubscriberManagerFactory">Фабрика ждунов</param>
    private TaskQueueManager(ITaskQueue defaultTaskQueue, IQueueSubscriberManagerFactory queueSubscriberManagerFactory)
    {
        Debug.Assert(defaultTaskQueue is not null, "defaultTaskQueue is not null");
        Debug.Assert(queueSubscriberManagerFactory is not null, "recordAwaiterManagerFactory is not null");

        _queueSubscriberManagerFactory = queueSubscriberManagerFactory;
        _queues = new Dictionary<QueueName, ITaskQueue>(QueueNameEqualityComparer.Instance)
        {
            [defaultTaskQueue.Name] = defaultTaskQueue
        };
    }

    /// <summary>
    /// Конструктор для менеджера, принимающий существующие очереди
    /// </summary>
    /// <param name="queues">Изначальные очереди, которые нужно хранить</param>
    /// <param name="queueSubscriberManagerFactory">Фабрика ждунов</param>
    /// <exception cref="ArgumentException">В переданной коллекции были ошибки: <br/>
    /// - Нет очереди по умолчанию
    /// - Есть несколько очередей по умолчанию
    /// - Есть несколько очередей с одинаковым названием
    /// </exception>
    /// <exception cref="ArgumentNullException">Какой-то объект в коллекции <paramref name="queues"/> - <c>null</c></exception>
    private TaskQueueManager(Dictionary<QueueName, ITaskQueue> queues,
                             IQueueSubscriberManagerFactory queueSubscriberManagerFactory)
    {
        _queueSubscriberManagerFactory = queueSubscriberManagerFactory;
        _queues = queues;
    }

    IReadOnlyCollection<IReadOnlyTaskQueue> IReadOnlyTaskQueueManager.GetAllQueues()
    {
        return GetAllQueues();
    }

    bool IReadOnlyTaskQueueManager.TryGetQueue(QueueName name, out IReadOnlyTaskQueue taskQueue)
    {
        return TryGetQueue(name, out taskQueue);
    }

    public bool TryGetQueue(QueueName name, out IReadOnlyTaskQueue taskQueue)
    {
        if (_queues.TryGetValue(name, out var queue))
        {
            taskQueue = queue;
            return true;
        }

        taskQueue = default!;
        return false;
    }

    public IReadOnlyCollection<ITaskQueue> GetAllQueues()
    {
        return _queues.Values;
    }

    public bool TryGetQueue(QueueName name, out ITaskQueue taskQueue)
    {
        if (_queues.TryGetValue(name, out taskQueue!))
        {
            return true;
        }

        taskQueue = default!;
        return false;
    }

    public bool TryAddQueue(QueueName name, ITaskQueue taskQueue)
    {
        Debug.Assert(taskQueue is not null);
        ArgumentNullException.ThrowIfNull(taskQueue);
        return _queues.TryAdd(name, taskQueue);
    }

    public ITaskQueueBuilder CreateBuilder(QueueName name, PriorityQueueCode code)
    {
        return new TaskQueueBuilder(name, code, _queueSubscriberManagerFactory);
    }

    public bool TryDeleteQueue(QueueName name, out ITaskQueue deleted)
    {
        return _queues.Remove(name, out deleted!);
    }

    public bool HasQueue(QueueName name)
    {
        return _queues.ContainsKey(name);
    }

    public IReadOnlyCollection<ITaskQueueMetadata> GetAllQueuesMetadata()
    {
        var result = new ITaskQueueMetadata[_queues.Values.Count];
        var i = 0;
        foreach (var value in _queues.Values)
        {
            result[i] = value.Metadata;
            i++;
        }

        return result;
    }

    /// <summary>
    /// Создать изначального менеджера очередей - с единственной очередью по умолчанию
    /// </summary>
    /// <param name="queueSubscriberManagerFactory">Фабрика ждунов для очередей</param>
    /// <returns>Новый <see cref="TaskQueueManager"/></returns>
    public static TaskQueueManager CreateDefault(IQueueSubscriberManagerFactory queueSubscriberManagerFactory)
    {
        return new TaskQueueManager(
            Defaults.CreateDefaultTaskQueue(queueSubscriberManagerFactory.CreateQueueSubscriberManager()),
            queueSubscriberManagerFactory);
    }

    public static TaskQueueManager CreateFrom(QueueCollection collection,
                                              IQueueSubscriberManagerFactory queueSubscriberManagerFactory)
    {
        var queues = CreateTaskQueueDictCheck(collection, queueSubscriberManagerFactory);
        return new TaskQueueManager(queues, queueSubscriberManagerFactory);
    }

    private static Dictionary<QueueName, ITaskQueue> CreateTaskQueueDictCheck(
        QueueCollection collection,
        IQueueSubscriberManagerFactory factory)
    {
        // Сразу инициализируем словарь нужным размером
        var result = new Dictionary<QueueName, ITaskQueue>(collection.Count, QueueNameEqualityComparer.Instance);

        // В процессе проверяем, что очередь по умолчанию (с пустым названием) существует
        var defaultQueueFound = false;

        foreach (var info in collection.GetAllQueues())
        {
            if (info.QueueName.IsDefaultQueue)
            {
                if (defaultQueueFound)
                {
                    throw new InvalidDataException("Найдено 2 очереди по умолчанию");
                }

                defaultQueueFound = true;
            }

            var queue = new TaskQueueBuilder(info.QueueName, info.Code, factory)
                       .WithMaxPayloadSize(info.MaxPayloadSize)
                       .WithMaxQueueSize(info.MaxQueueSize)
                       .WithPriorityRange(info.PriorityRange)
                       .WithData(info.Data)
                       .Build();

            try
            {
                result.Add(info.QueueName, queue);
            }
            catch (ArgumentException ae)
            {
                throw new ArgumentException(
                    $"В переданной коллекции очередей найдено 2 очереди с одинаковым названием: {queue.Name}", ae);
            }
        }


        if (!defaultQueueFound)
        {
            throw new ArgumentException($"В переданной коллекции очередей нет очереди по умолчанию");
        }

        return result;
    }
}