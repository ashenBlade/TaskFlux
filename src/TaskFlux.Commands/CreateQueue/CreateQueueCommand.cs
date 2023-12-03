using TaskFlux.Commands.Error;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Models;
using TaskFlux.PriorityQueue;
using TaskFlux.Serialization;

namespace TaskFlux.Commands.CreateQueue;

public class CreateQueueCommand : UpdateCommand
{
    public override CommandType Type => CommandType.CreateQueue;
    public QueueName Queue { get; }
    public PriorityQueueCode Code { get; }
    public int? MaxQueueSize { get; }
    public int? MaxPayloadSize { get; }
    public (long, long)? PriorityRange { get; }

    private ITaskQueue CreateTaskQueue()
    {
        var builder = new TaskQueueBuilder(Queue, Code);

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

    public CreateQueueCommand(QueueName queue,
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

        Queue = queue;
        Code = code;
        MaxQueueSize = maxQueueSize;
        MaxPayloadSize = maxPayloadSize;
        PriorityRange = priorityRange;
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
        catch (InvalidOperationException ioe)
        {
            return new ErrorResponse(ErrorType.InvalidQueueParameters, ioe.Message);
        }

        if (manager.TryAddQueue(Queue, queue))
        {
            return OkResponse.Instance;
        }

        return new ErrorResponse(ErrorType.Unknown, "Не удалось создать указанную очередь. Неизвестная ошибка");
    }

    public override void ApplyNoResult(IApplication context)
    {
        var manager = context.TaskQueueManager;
        if (!manager.HasQueue(Queue))
        {
            return;
        }

        var queue = CreateTaskQueue();
        manager.TryAddQueue(Queue, queue);
    }

    public override bool TryGetDelta(out Delta delta)
    {
        delta = new CreateQueueDelta(Queue, Code, MaxQueueSize, MaxPayloadSize, PriorityRange);
        return true;
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