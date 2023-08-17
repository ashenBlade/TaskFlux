using System.Diagnostics;
using JobQueue.Core;
using JobQueue.PriorityQueue;

namespace JobQueue.InMemory;

public class SimpleJobQueueManager: IJobQueueManager
{
    private readonly Dictionary<string, IJobQueue> _queues;

    public SimpleJobQueueManager(string defaultQueueName, IJobQueue jobQueue)
    {
        _queues = new()
        {
            [defaultQueueName] = jobQueue
        };
    }
    
    public bool TryGetQueue(QueueName name, out IJobQueue jobQueue)
    {
        if (_queues.TryGetValue(name.Name, out jobQueue!))
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
        return _queues.TryAdd(name.Name, jobQueue);
    }

    public bool TryDeleteQueue(QueueName name, out IJobQueue deleted)
    {
        return _queues.Remove(name.Name, out deleted!);
    }
}