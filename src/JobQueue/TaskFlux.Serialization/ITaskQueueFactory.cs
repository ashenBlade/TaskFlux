using TaskFlux.Core;
using TaskFlux.Models;

namespace TaskFlux.Serialization;

public interface ITaskQueueFactory
{
    public ITaskQueue CreateTaskQueue(QueueName name,
                                      int? maxSize,
                                      (long Min, long Max)? priorityRange,
                                      int? maxPayloadSize,
                                      IReadOnlyCollection<(long Key, byte[] Value)> payload);
}