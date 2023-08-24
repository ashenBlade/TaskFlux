using System.Diagnostics;
using JobQueue.Core;

namespace JobQueue.InMemory;

public class SimpleJobQueueManager : IJobQueueManager
{
    // Если решим добавить одну очередь, но под разными именами, то такое нужно обрабатывать правильно
    public int QueuesCount => _queues.Count;

    public IEnumerable<IJobQueue> GetAllQueues()
    {
        return _queues.Values;
    }

    private readonly Dictionary<QueueName, IJobQueue> _queues;

    public SimpleJobQueueManager(IJobQueue defaultJobQueue)
    {
        _queues = new(QueueNameEqualityComparer.Instance)
        {
            [defaultJobQueue.Name] = defaultJobQueue
        };
    }

    /// <summary>
    /// Конструктор для менеджера очередей, принимающий несколько очередей
    /// </summary>
    /// <param name="jobQueues">Изначальные очереди, которые нужно хранить</param>
    /// <exception cref="ArgumentException">В переданной коллекции были ошибки: <br/>
    /// - Нет очереди по умолчанию
    /// - Есть несколько очередей по умолчанию
    /// - Есть несколько очередей с одинаковым названием
    /// </exception>
    /// <exception cref="ArgumentNullException">Какой-то объект в коллекции <paramref name="jobQueues"/> - <c>null</c></exception>
    public SimpleJobQueueManager(IReadOnlyCollection<IJobQueue> jobQueues)
    {
        _queues = CreateJobQueueDictCheck(jobQueues);
    }

    private static Dictionary<QueueName, IJobQueue> CreateJobQueueDictCheck(IReadOnlyCollection<IJobQueue> jobQueues)
    {
        // Предполагаю правильное исполнение без ошибок - инициализирую словарь сразу нужного размера
        var result = new Dictionary<QueueName, IJobQueue>(jobQueues.Count, QueueNameEqualityComparer.Instance);
        
        var found = false;
        foreach (var jobQueue in jobQueues)
        {
            try
            {
                result.Add(jobQueue.Name, jobQueue);
            }
            catch (ArgumentException arg)
            {
                throw new ArgumentException(
                    $"В переданной коллекции очередей найдено 2 очереди с одинаковым названием: {jobQueue.Name}", arg);
            }
            catch (NullReferenceException)
            {
                throw new ArgumentNullException(
                    $"В переданной коллекции очередей {nameof(jobQueues)} обнаружен null");
            }
        }

        if (!found)
        {
            throw new ArgumentException(
                $"В переданной коллекции очередей {nameof(jobQueues)} нет очереди по умолчанию");
        }

        return result;
    }

    public bool TryGetQueue(QueueName name, out IJobQueue jobQueue)
    {
        if (_queues.TryGetValue(name, out jobQueue!))
        {
            return true;
        }

        jobQueue = default!;
        return false;
    }

    public bool TryAddQueue(QueueName name, IJobQueue jobQueue)
    {
        Debug.Assert(jobQueue is not null);
        ArgumentNullException.ThrowIfNull(jobQueue);
        return _queues.TryAdd(name, jobQueue);
    }

    public bool TryDeleteQueue(QueueName name, out IJobQueue deleted)
    {
        return _queues.Remove(name, out deleted!);
    }

    public bool HasQueue(QueueName name)
    {
        return _queues.ContainsKey(name);
    }

    public IReadOnlyCollection<IJobQueueMetadata> GetAllQueuesMetadata()
    {
        // Может поменять на IEnumerable и лениво вычислять через yield?
        var result = new IJobQueueMetadata[QueuesCount];
        var i = 0;
        foreach (var value in _queues.Values)
        {
            result[i] = value.Metadata;
        }

        return result;
    }
}