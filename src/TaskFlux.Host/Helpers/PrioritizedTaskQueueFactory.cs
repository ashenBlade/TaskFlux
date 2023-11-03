using TaskQueue.Core;
using TaskQueue.InMemory;
using TaskQueue.PriorityQueue.StandardLibrary;
using TaskQueue.Serialization;

namespace TaskFlux.Host.Helpers;

public class PrioritizedTaskQueueFactory : ITaskQueueFactory
{
    public static readonly PrioritizedTaskQueueFactory Instance = new();

    public ITaskQueue CreateTaskQueue(QueueName name,
                                      uint maxSize,
                                      IReadOnlyCollection<(long Priority, byte[] Payload)> data)
    {
        var queue = new StandardLibraryPriorityQueue<long, byte[]>();
        foreach (var (priority, payload) in data)
        {
            queue.Enqueue(priority, payload);
        }

        return new PrioritizedTaskQueue(name, maxSize, queue);
    }
}