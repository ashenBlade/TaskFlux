using TaskFlux.Commands.Error;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Delta;
using TaskFlux.Models;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Commands.CreateQueue;

public class CreateQueueCommand : UpdateCommand
{
    public override CommandType Type => CommandType.CreateQueue;
    public QueueName Name { get; }
    public PriorityQueueCode Code { get; }
    public int? MaxQueueSize { get; }
    public int? MaxPayloadSize { get; }
    public (long, long)? PriorityRange { get; }

    private ITaskQueue CreateTaskQueue()
    {
        var builder = new TaskQueueBuilder(Name, Code);

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
                              PriorityQueueCode code,
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

        if (code is PriorityQueueCode.QueueArray && priorityRange is null)
        {
            throw new ArgumentException("Необходимо указать диапазон ключей для списка очередей");
        }

        Name = name;
        Code = code;
        MaxQueueSize = maxQueueSize;
        MaxPayloadSize = maxPayloadSize;
        PriorityRange = priorityRange;
    }

    public override Response Apply(IApplication context)
    {
        var manager = context.TaskQueueManager;
        if (manager.HasQueue(Name))
        {
            return DefaultErrors.QueueAlreadyExists;
        }

        ITaskQueue queue;
        try
        {
            queue = CreateTaskQueue();
        }
        catch (InvalidOperationException ioe)
        {
            return new ErrorResponse(ErrorType.InvalidQueueParameters, ioe.Message);
        }

        if (manager.TryAddQueue(Name, queue))
        {
            return OkResponse.Instance;
        }

        return new ErrorResponse(ErrorType.Unknown, "Не удалось создать указанную очередь. Неизвестная ошибка");
    }

    public override void ApplyNoResult(IApplication context)
    {
        var manager = context.TaskQueueManager;
        if (!manager.HasQueue(Name))
        {
            return;
        }

        var queue = CreateTaskQueue();
        manager.TryAddQueue(Name, queue);
    }

    public override bool TryGetDelta(out Delta.Delta delta)
    {
        var implementation = ( int ) Code;
        var maxQueueSize = MaxQueueSize ?? -1;
        var maxMessageSize = MaxPayloadSize ?? -1;
        delta = new CreateQueueDelta(Name, implementation, maxQueueSize, maxMessageSize, PriorityRange);
        return true;
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