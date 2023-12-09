using TaskFlux.Commands.Error;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Models;

namespace TaskFlux.Commands.DeleteQueue;

public class DeleteQueueCommand : ModificationCommand
{
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

    public override void Accept(ICommandVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(ICommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}