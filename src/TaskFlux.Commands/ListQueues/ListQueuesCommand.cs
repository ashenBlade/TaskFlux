using JobQueue.Core;
using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.ListQueues;

public class ListQueuesCommand: Command
{
    public override CommandType Type => CommandType.ListQueues;
    public static readonly ListQueuesCommand Instance = new();

    public override Result Apply(ICommandContext context)
    {
        var manager = context.Node.GetJobQueueManager();
        var result = manager.GetAllQueuesMetadata();
        return new ListQueuesResult(result);
    }

    public override void ApplyNoResult(ICommandContext context)
    { }

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