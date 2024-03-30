using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Core.Queue;

namespace TaskFlux.Core.Commands.Dequeue;

public class AwaitableDequeueCommand : ModificationCommand
{
    /// <summary>
    /// Очередь, из которой необходимо читать записи
    /// </summary>
    public QueueName Queue { get; }

    /// <summary>
    /// Таймаут ожидания новых записей
    /// </summary>
    public TimeSpan Timeout { get; }

    /// <summary>
    /// Следует ли сохранять значение сразу же в случае успешного прочтения
    /// </summary>
    public bool Persistent { get; }

    private AwaitableDequeueCommand(QueueName queue, TimeSpan timeout, bool persistent)
    {
        Queue = queue;
        Timeout = timeout;
        Persistent = persistent;
    }

    public override Response Apply(IApplication application)
    {
        if (!application.TaskQueueManager.TryGetQueue(Queue, out var queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        if (queue.TryDequeue(out var record))
        {
            return CreateDequeueResponse(record);
        }

        return new QueueSubscriberResponse(queue.Subscribe(), Queue, Timeout);
    }

    public override void Accept(ICommandVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(ICommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }

    private DequeueResponse CreateDequeueResponse(QueueRecord record) =>
        Persistent
            ? DequeueResponse.CreatePersistent(Queue, record.Priority, record.Payload)
            : DequeueResponse.CreateNonPersistent(Queue, record.Priority, record.Payload);

    public static AwaitableDequeueCommand CreatePersistent(QueueName queue, TimeSpan timeout) =>
        new(queue, timeout, true);

    public static AwaitableDequeueCommand CreateNonPersistent(QueueName queue, TimeSpan timeout) =>
        new(queue, timeout, false);
}