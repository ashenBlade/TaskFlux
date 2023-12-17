namespace TaskFlux.Core.Queue;

public static class TaskQueueBuilderExtensions
{
    public static TaskQueueBuilder WithPriorityRange(this TaskQueueBuilder builder, (long, long)? priorityRange)
    {
        if (priorityRange is var (min, max))
        {
            return builder.WithPriorityRange(min, max);
        }

        return builder;
    }
}