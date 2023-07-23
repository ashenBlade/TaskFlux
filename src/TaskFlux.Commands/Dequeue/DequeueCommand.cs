using TaskFlux.Core;

namespace TaskFlux.Commands.Dequeue;

public class DequeueCommand: Command
{
    public static readonly DequeueCommand Instance = new();
    public override CommandType Type => CommandType.Dequeue;
    public override Result Apply(ICommandContext context)
    {
        var queue = context.Node.GetJobQueue();
        if (queue.TryDequeue(out var key, out var payload))
        {
            return DequeueResult.Create(key, payload);
        }
        
        return DequeueResult.Empty;
    }

    public override void ApplyNoResult(ICommandContext context)
    {
        context.Node
               .GetJobQueue()
               .TryDequeue(out _, out _);
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