using TaskFlux.Core.Commands.Ok;
using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Dequeue;

/// <summary>
/// Команда для фиксации чтения записи из очереди
/// </summary>
public class CommitDequeueCommand : ModificationCommand
{
    public DequeueResponse Response { get; }

    public CommitDequeueCommand(DequeueResponse response)
    {
        Response = response;
    }

    public override Response Apply(IApplication application)
    {
        // Эта команда необходима только для репликации самой команды - в ней нет логики
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