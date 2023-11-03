using TaskQueue.Core;

namespace TaskQueue.Serialization.Tests;

public class StubTaskQueueFactory : ITaskQueueFactory
{
    public ITaskQueue CreateTaskQueue(QueueName name,
                                      uint maxSize,
                                      IReadOnlyCollection<(long Priority, byte[] Payload)> data)
    {
        return new StubTaskQueue(name, maxSize, data);
    }
}