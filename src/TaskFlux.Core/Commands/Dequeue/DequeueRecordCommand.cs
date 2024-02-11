using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Dequeue;

public class DequeueRecordCommand : ModificationCommand
{
    // Конкретно для этой команды мы используем быстрый путь выполнения - без фиксации результата.
    // Это нужно для использования Ack/Nack команд (at-least-once семантики) - изменения будут зафиксированы другими командами
    public QueueName Queue { get; }

    public DequeueRecordCommand(QueueName queue)
    {
        Queue = queue;
    }

    public override Response Apply(IApplication context)
    {
        var manager = context.TaskQueueManager;

        if (!manager.TryGetQueue(Queue, out var queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        if (queue.TryDequeue(out var key, out var payload))
        {
            return DequeueResponse.Create(Queue, key, payload);
        }

        return DequeueResponse.Empty;
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