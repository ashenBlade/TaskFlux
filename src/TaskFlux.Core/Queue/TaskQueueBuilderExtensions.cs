namespace TaskFlux.Core.Queue;

public static class TaskQueueBuilderExtensions
{
    public static ITaskQueueBuilder WithPriorityRange(this ITaskQueueBuilder builder, (long, long)? priorityRange)
    {
        if (priorityRange is var (min, max))
        {
            return builder.WithPriorityRange(min, max);
        }

        return builder;
    }
}