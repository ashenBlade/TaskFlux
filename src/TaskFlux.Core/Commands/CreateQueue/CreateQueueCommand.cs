using TaskFlux.Core.Commands.CreateQueue.ImplementationDetails;
using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.Ok;
using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Core.Queue;

namespace TaskFlux.Core.Commands.CreateQueue;

public class CreateQueueCommand : ModificationCommand
{
    public QueueName Queue { get; }
    public QueueImplementationDetails Details { get; }

    private ITaskQueue CreateTaskQueue()
    {
        var builder = new TaskQueueBuilder(Queue, Details.Code);

        if (Details.TryGetMaxQueueSize(out var maxQueueSize))
        {
            builder.WithMaxQueueSize(maxQueueSize);
        }

        if (Details.TryGetMaxPayloadSize(out var maxPayloadSize))
        {
            builder.WithMaxPayloadSize(maxPayloadSize);
        }

        if (Details.TryGetPriorityRange(out var min, out var max))
        {
            builder.WithPriorityRange(min, max);
        }

        return builder.Build();
    }

    /// <summary>
    /// Конструктор для команды создания очереди
    /// </summary>
    /// <param name="queue">Название очереди, которое нужно создать</param>
    /// <param name="details">Детали реализации очереди</param>
    public CreateQueueCommand(QueueName queue, QueueImplementationDetails details)
    {
        ArgumentNullException.ThrowIfNull(details);
        Queue = queue;
        Details = details;
    }

    public override Response Apply(IApplication context)
    {
        var manager = context.TaskQueueManager;
        if (manager.HasQueue(Queue))
        {
            return DefaultErrors.QueueAlreadyExists;
        }

        ITaskQueue queue;
        try
        {
            queue = CreateTaskQueue();
        }
        catch (InvalidOperationException)
        {
            /*
             * Все значения мы проверяем еще на этапе создания QueueImplementationDetails,
             * поэтому этот вариант маловероятен, но все же лучше перебздеть, чем недобздеть
             */
            return new ErrorResponse(ErrorType.Unknown, "Переданные для создания очереди пар*аметры некорректны");
        }

        if (manager.TryAddQueue(Queue, queue))
        {
            return OkResponse.Instance;
        }

        return new ErrorResponse(ErrorType.Unknown, "Не удалось создать указанную очередь. Неизвестная ошибка");
    }

    public override void Accept(ICommandVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(ICommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}