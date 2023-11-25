using System.Diagnostics;
using TaskFlux.Models;

namespace TaskFlux.Core.Queue;

public class TaskQueueManager : ITaskQueueManager
{
    // Если решим добавить одну очередь, но под разными именами, то такое нужно обрабатывать правильно
    public int QueuesCount => _queues.Count;

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

    private readonly Dictionary<QueueName, ITaskQueue> _queues;

    public TaskQueueManager(ITaskQueue defaultTaskQueue)
    {
        _queues = new Dictionary<QueueName, ITaskQueue>(QueueNameEqualityComparer.Instance)
        {
            [defaultTaskQueue.Name] = defaultTaskQueue
        };
    }

    /// <summary>
    /// Конструктор для менеджера очередей, принимающий несколько очередей
    /// </summary>
    /// <param name="queues">Изначальные очереди, которые нужно хранить</param>
    /// <exception cref="ArgumentException">В переданной коллекции были ошибки: <br/>
    /// - Нет очереди по умолчанию
    /// - Есть несколько очередей по умолчанию
    /// - Есть несколько очередей с одинаковым названием
    /// </exception>
    /// <exception cref="ArgumentNullException">Какой-то объект в коллекции <paramref name="queues"/> - <c>null</c></exception>
    public TaskQueueManager(IReadOnlyCollection<ITaskQueue> queues)
    {
        _queues = CreateTaskQueueDictCheck(queues);
    }

    private static Dictionary<QueueName, ITaskQueue> CreateTaskQueueDictCheck(IReadOnlyCollection<ITaskQueue> queues)
    {
        // Предполагаю правильное исполнение без ошибок - инициализирую словарь сразу нужного размера
        var result = new Dictionary<QueueName, ITaskQueue>(queues.Count, QueueNameEqualityComparer.Instance);

        var found = false;
        foreach (var queue in queues)
        {
            try
            {
                if (!found && queue.Name.IsDefaultQueue)
                {
                    Debug.Assert(!found, "Найдено 2 очереди по умолчанию");
                    found = true;
                }

                result.Add(queue.Name, queue);
            }
            catch (ArgumentException arg)
            {
                throw new ArgumentException(
                    $"В переданной коллекции очередей найдено 2 очереди с одинаковым названием: {queue.Name}", arg);
            }
            catch (NullReferenceException)
            {
                throw new ArgumentNullException($"В переданной коллекции очередей {nameof(queues)} обнаружен null");
            }
        }

        if (!found)
        {
            throw new ArgumentException($"В переданной коллекции очередей {nameof(queues)} нет очереди по умолчанию");
        }

        return result;
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
}