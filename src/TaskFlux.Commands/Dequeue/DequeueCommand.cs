using System.Diagnostics;
using JobQueue.Core;
using TaskFlux.Commands.Error;
using TaskFlux.Core;

namespace TaskFlux.Commands.Dequeue;

public class DequeueCommand: Command
{
    public DequeueCommand(string queue)
    {
        Queue = queue;
    }

    public string Queue { get; }
    public override CommandType Type => CommandType.Dequeue;
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
        
        if (queue.TryDequeue(out var key, out var payload))
        {
            return DequeueResult.Create(key, payload);
        }
        
        return DequeueResult.Empty;
    }

    public override void ApplyNoResult(ICommandContext context)
    {
        if (!QueueName.TryParse(Queue, out var queueName))
        {
            return;
        }
        
        var manager = context.Node.GetJobQueueManager();

        if (!manager.TryGetQueue(queueName, out var queue))
        {
            return;
        }

        queue.TryDequeue(out _, out _);
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