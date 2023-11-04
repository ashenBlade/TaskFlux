using TaskQueue.Core;

namespace TaskQueue.Serialization.Tests;

public class StubTaskQueueFactory : ITaskQueueFactory
{
    public ITaskQueue CreateTaskQueue(QueueName name,
                                      int? maxSize,
                                      (long Min, long Max)? priorityRange,
                                      int? maxPayloadSize,
                                      IReadOnlyCollection<(long Key, byte[] Value)> payload)
    {
        return new StubTaskQueue(name, maxSize, priorityRange, maxPayloadSize, payload);
    }
}