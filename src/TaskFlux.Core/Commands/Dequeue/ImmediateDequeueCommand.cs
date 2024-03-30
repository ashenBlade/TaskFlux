using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Dequeue;

public class ImmediateDequeueCommand : ModificationCommand
{
    // Конкретно для этой команды мы используем быстрый путь выполнения - без фиксации результата.
    // Это нужно для использования Ack/Nack команд (at-least-once семантики) - изменения будут зафиксированы другими командами
    /// <summary>
    /// Название очереди, из которой необходимо прочитать запись
    /// </summary>
    public QueueName Queue { get; }

    /// <summary>
    /// Следует ли сразу сохранять результат чтения или дополнительно подтверждать (ACK/NACK) 
    /// </summary>
    public bool Persistent { get; }

    private ImmediateDequeueCommand(QueueName queue, bool persistent)
    {
        Queue = queue;
        Persistent = persistent;
    }

    public override Response Apply(IApplication context)
    {
        var manager = context.TaskQueueManager;

        if (!manager.TryGetQueue(Queue, out var queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        if (queue.TryDequeue(out var record))
        {
            return GetDequeueResponse(record.Priority, record.Payload);
        }

        return DequeueResponse.Empty;
    }

    private DequeueResponse GetDequeueResponse(long key, byte[] payload)
        => Persistent
               ? DequeueResponse.CreatePersistent(Queue, key, payload)
               : DequeueResponse.CreateNonPersistent(Queue, key, payload);

    public override void Accept(ICommandVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(ICommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }

    public static ImmediateDequeueCommand CreateNonPersistent(QueueName queue) => new(queue, persistent: false);

    public static ImmediateDequeueCommand CreatePersistent(QueueName queue) => new(queue, persistent: true);
}