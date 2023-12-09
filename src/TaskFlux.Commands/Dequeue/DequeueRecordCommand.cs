using TaskFlux.Commands.Error;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Models;

namespace TaskFlux.Commands.Dequeue;

public class DequeueRecordCommand : ModificationCommand
{
    /// <summary>
    /// Флаг перманентности операции.
    /// Если равен <c>true</c>, то сразу произойдет коммит операции удаления,
    /// иначе необходимо вручную удалять запись (<see cref="CommitDequeueCommand"/>),
    /// либо потом вернуть обратно (<see cref="ReturnRecordCommand"/>)
    /// </summary>
    private readonly bool _permanent;

    // Конкретно для этой команды мы используем быстрый путь выполнения - без фиксации результата.
    // Это нужно для использования Ack/Nack команд (at-least-once семантики) - изменения будут зафиксированы другими командами
    public QueueName Queue { get; }

    public DequeueRecordCommand(QueueName queue, bool permanent)
    {
        _permanent = permanent;
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
            return DequeueResponse.Create(Queue, key, payload, _permanent);
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