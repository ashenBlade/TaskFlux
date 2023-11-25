using TaskFlux.Commands.Error;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Models;
using TaskFlux.Serialization;

namespace TaskFlux.Commands.DeleteQueue;

public class DeleteQueueCommand : UpdateCommand
{
    public override CommandType Type => CommandType.DeleteQueue;
    public QueueName QueueName { get; }

    public DeleteQueueCommand(QueueName queueName)
    {
        QueueName = queueName;
    }

    public override Response Apply(IApplication context)
    {
        var manager = context.TaskQueueManager;
        if (!manager.HasQueue(QueueName))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        if (manager.TryDeleteQueue(QueueName, out _))
        {
            return OkResponse.Instance;
        }

        return new ErrorResponse(ErrorType.Unknown, "Неизвестная ошибка при удалении очереди. Очередь не была удалена");
    }

    public override void ApplyNoResult(IApplication context)
    {
        var manager = context.TaskQueueManager;
        if (!manager.HasQueue(QueueName))
        {
            return;
        }

        manager.TryDeleteQueue(QueueName, out _);
    }

    public override bool TryGetDelta(out Delta delta)
    {
        delta = new DeleteQueueDelta(QueueName);
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