using JobQueue.Core;
using JobQueue.InMemory;
using JobQueue.PriorityQueue.StandardLibrary;
using JobQueue.Serialization;

namespace TaskFlux.Host.Helpers;

public class PrioritizedJobQueueFactory : IJobQueueFactory
{
    public static readonly PrioritizedJobQueueFactory Instance = new();
    public IJobQueue CreateJobQueue(QueueName name,
                                    uint maxSize,
                                    IReadOnlyCollection<(long Priority, byte[] Payload)> data)
    {
        var queue = new StandardLibraryPriorityQueue<long, byte[]>();
        foreach (var (priority, payload) in data)
        {
            queue.Enqueue(priority, payload);
        }

        return new PrioritizedJobQueue(name, maxSize, queue);
    }
}