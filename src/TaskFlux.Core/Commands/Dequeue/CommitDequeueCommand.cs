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
        // Логика коммита содержится непосредственно в DequeueResponse
        return Response;
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