using JobQueue.Core;
using TaskFlux.Commands.Error;

namespace TaskFlux.Commands.Count;

public class CountCommand: Command
{
    public string Queue { get; }
    public override CommandType Type => CommandType.Count;

    public CountCommand(string queue)
    {
        Queue = queue;
    }
    
    public override Result Apply(ICommandContext context)
    {
        if (!QueueName.TryParse(Queue, out var queueName))
        {
            return DefaultErrors.InvalidQueueName;
        }

        var manager = context.Node.GetJobQueueManager();

        if (!manager.TryGetQueue(queueName, out var queue))
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

    public override void ApplyNoResult(ICommandContext context)
    { }

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