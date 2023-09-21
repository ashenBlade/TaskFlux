using JobQueue.Core;

namespace JobQueue.Serialization.Tests;

public class StubJobQueueFactory : IJobQueueFactory
{
    public IJobQueue CreateJobQueue(QueueName name,
                                    uint maxSize,
                                    IReadOnlyCollection<(long Priority, byte[] Payload)> data)
    {
        return new StubJobQueue(name, maxSize, data);
    }
}