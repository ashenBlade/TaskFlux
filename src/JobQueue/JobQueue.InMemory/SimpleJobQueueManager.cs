using System.Diagnostics;
using JobQueue.Core;
using JobQueue.PriorityQueue;

namespace JobQueue.InMemory;

public class SimpleJobQueueManager: IJobQueueManager
{
    // Если решим добавить одну очередь, но под разными именами, то такое нужно обрабатывать правильно
    public int QueuesCount => _queues.Count;
    
    private readonly Dictionary<QueueName, IJobQueue> _queues;

    public SimpleJobQueueManager(IJobQueue defaultJobQueue)
    {
        _queues = new(QueueNameEqualityComparer.Instance)
        {
            [defaultJobQueue.Name] = defaultJobQueue
        };
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