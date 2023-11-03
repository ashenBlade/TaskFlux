using TaskFlux.Commands.Error;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;
using TaskQueue.Core;

namespace TaskFlux.Commands.DeleteQueue;

public class DeleteQueueCommand : UpdateCommand
{
    public override CommandType Type => CommandType.DeleteQueue;
    public QueueName QueueName { get; }

    public DeleteQueueCommand(QueueName queueName)
    {
        QueueName = queueName;
    }

    public override Result Apply(ICommandContext context)
    {
        var manager = context.Node.GetTaskQueueManager();
        if (!manager.HasQueue(QueueName))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        if (manager.TryDeleteQueue(QueueName, out _))
        {
            return OkResult.Instance;
        }

        return new ErrorResult(ErrorType.Unknown, "Неизвестная ошибка при удалении очереди. Очередь не была удалена");
    }

    public override void ApplyNoResult(ICommandContext context)
    {
        var manager = context.Node.GetTaskQueueManager();
        if (!manager.HasQueue(QueueName))
        {
            return;
        }

        manager.TryDeleteQueue(QueueName, out _);
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