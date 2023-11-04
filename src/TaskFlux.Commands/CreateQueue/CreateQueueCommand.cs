using TaskFlux.Commands.Error;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;
using TaskQueue.Core;

namespace TaskFlux.Commands.CreateQueue;

public class CreateQueueCommand : UpdateCommand
{
    public override CommandType Type => CommandType.CreateQueue;
    public QueueName Name { get; }
    public int? MaxQueueSize { get; }
    public int? MaxPayloadSize { get; }
    public (long, long)? PriorityRange { get; }

    private ITaskQueue CreateTaskQueue()
    {
        var builder = new TaskQueueBuilder(Name);

        if (MaxQueueSize is { } maxQueueSize)
        {
            builder.WithMaxQueueSize(maxQueueSize);
        }

        if (MaxPayloadSize is { } maxPayloadSize)
        {
            builder.WithMaxPayloadSize(maxPayloadSize);
        }

        if (PriorityRange is var (min, max))
        {
            builder.WithPriorityRange(min, max);
        }

        return builder.Build();
    }

    public CreateQueueCommand(QueueName name,
                              int? maxQueueSize,
                              int? maxPayloadSize,
                              (long, long)? priorityRange)
    {
        if (priorityRange is var (min, max) && max < min)
        {
            throw new ArgumentException("Минимальное значение ключа не может быть больше максимального");
        }

        if (maxPayloadSize is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxPayloadSize), maxPayloadSize,
                "Максимальный размер сообщения не может быть отрицательным значением");
        }

        if (maxQueueSize is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxQueueSize), maxQueueSize,
                "Максимальный размер очереди не может быть отрицательным значением");
        }

        Name = name;
        MaxQueueSize = maxQueueSize;
        MaxPayloadSize = maxPayloadSize;
        PriorityRange = priorityRange;
    }

    public override Result Apply(ICommandContext context)
    {
        var manager = context.Node.GetTaskQueueManager();
        if (manager.HasQueue(Name))
        {
            return DefaultErrors.QueueAlreadyExists;
        }

        var queue = CreateTaskQueue();
        if (manager.TryAddQueue(Name, queue))
        {
            return OkResult.Instance;
        }

        return new ErrorResult(ErrorType.Unknown, "Не удалось создать указанную очередь. Неизвестная ошибка");
    }

    public override void ApplyNoResult(ICommandContext context)
    {
        var manager = context.Node.GetTaskQueueManager();
        if (!manager.HasQueue(Name))
        {
            return;
        }

        var queue = CreateTaskQueue();
        manager.TryAddQueue(Name, queue);
    }

    public override void Accept(ICommandVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(IReturningCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncCommandVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}