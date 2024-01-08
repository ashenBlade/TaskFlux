using TaskFlux.Commands.Error;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Models;

namespace TaskFlux.Commands.DeleteQueue;

public class DeleteQueueCommand : ModificationCommand
{
    public QueueName Queue { get; }

    public DeleteQueueCommand(QueueName queue)
    {
        Queue = queue;
    }

    public override Response Apply(IApplication context)
    {
        var manager = context.TaskQueueManager;
        if (!manager.HasQueue(Queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        if (manager.TryDeleteQueue(Queue, out _))
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