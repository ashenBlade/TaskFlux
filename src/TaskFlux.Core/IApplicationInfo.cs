using TaskQueue.Core;

namespace TaskFlux.Core;

public interface IApplicationInfo
{
    public Version Version { get; }
    public QueueName DefaultQueueName { get; }
}