using TaskFlux.Core.Commands.Ok;
using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Domain;

namespace TaskFlux.Core.Commands.Dequeue;

/// <summary>
/// Команда для возвращения ранее прочитанной записи обратно в очередь
/// </summary>
public class ReturnRecordCommand : Command
{
    public QueueName Queue { get; }

    /// <summary>
    /// Ранее полученный результат выполнения Dequeue команды
    /// </summary>
    public QueueRecord Record { get; }

    // Просто возвращаем значение обратно - коммитить ничего не надо
    public ReturnRecordCommand(QueueName queue, QueueRecord record)
    {
        Queue = queue;
        Record = record;
    }

    public override Response Apply(IApplication application)
    {
        // Пока клиент обрабатывал сообщение, очередь могли удалить
        if (application.TaskQueueManager.TryGetQueue(Queue, out var queue))
        {
            queue.EnqueueExisting(Record);
        }

        /*
         * Пока в любом случае, возвращаю ОК.
         * Но может стоит возвращать какое-нибудь отдельное сообщение по типу "очередь удалили"
         */
        return OkResponse.Instance;
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