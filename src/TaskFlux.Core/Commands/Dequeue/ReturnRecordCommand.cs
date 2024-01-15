using TaskFlux.Core.Commands.Ok;
using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Dequeue;

/// <summary>
/// Команда для возвращения ранее прочитанной записи обратно в очередь
/// </summary>
public class ReturnRecordCommand : Command
{
    /// <summary>
    /// Ранее полученный результат выполнения Dequeue команды
    /// </summary>
    public DequeueResponse Response { get; }

    // Просто возвращаем значение обратно - коммитить ничего не надо

    public ReturnRecordCommand(DequeueResponse response)
    {
        Response = response;
    }

    public override Response Apply(IApplication application)
    {
        if (Response.TryGetResult(out var queueName, out var key, out var message)
         && application.TaskQueueManager.TryGetQueue(queueName, out var queue))
        {
            queue.Enqueue(key, message);
        }

        // Если не смогли получить очередь, то это значит, что она была удалена между чтением и коммитом.
        // Пока на такое не реагирую, но возможно стоит возвращать какую-нибудь ошибки или типа того
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