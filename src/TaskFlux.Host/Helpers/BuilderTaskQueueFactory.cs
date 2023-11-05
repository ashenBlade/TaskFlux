using TaskFlux.Core;
using TaskFlux.Models;
using TaskFlux.Serialization;

namespace TaskFlux.Host.Helpers;

public class BuilderTaskQueueFactory : ITaskQueueFactory
{
    public static readonly BuilderTaskQueueFactory Instance = new();

    public ITaskQueue CreateTaskQueue(QueueName name,
                                      int? maxSize,
                                      (long Min, long Max)? priorityRange,
                                      int? maxPayloadSize,
                                      IReadOnlyCollection<(long Key, byte[] Value)> payload)
    {
        var builder = new TaskQueueBuilder(name);
        if (maxSize is { } ms)
        {
            builder.WithMaxQueueSize(ms);
        }

        if (priorityRange is var (min, max))
        {
            builder.WithPriorityRange(min, max);
        }

        if (maxPayloadSize is { } mps)
        {
            builder.WithMaxPayloadSize(mps);
        }

        builder.WithData(payload);

        return builder.Build();
    }
}