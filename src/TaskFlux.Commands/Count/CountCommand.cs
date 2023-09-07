using JobQueue.Core;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.Count;

public class CountCommand : ReadOnlyCommand
{
    public QueueName Queue { get; }
    public override CommandType Type => CommandType.Count;

    public CountCommand(QueueName queue)
    {
        Queue = queue;
    }

    protected override Result Apply(IReadOnlyCommandContext context)
    {
        var manager = context.Node.GetJobQueueManager();

        if (!manager.TryGetQueue(Queue, out var queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }

        var count = queue.Count;
        if (count == 0)
        {
            return CountResult.Empty;
        }

        return new CountResult(count);
    }

    protected override void ApplyNoResult(IReadOnlyCommandContext context)
    {
    }

    public override void Accept(ICommandVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncCommandVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }

    public override T Accept<T>(IReturningCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}