using JobQueue.Core;
using TaskFlux.Commands.Error;

namespace TaskFlux.Commands.Enqueue;

public class EnqueueCommand: Command
{
    public QueueName Queue { get; }
    public long Key { get; }
    public byte[] Payload { get; }

    public EnqueueCommand(long key, byte[] payload, QueueName queue)
    {
        ArgumentNullException.ThrowIfNull(payload);
        Key = key;
        Payload = payload;
        Queue = queue;
    }
    
    public override CommandType Type => CommandType.Enqueue;
    public override Result Apply(ICommandContext context)
    {
        var manager = context.Node.GetJobQueueManager();

        if (!manager.TryGetQueue(Queue, out var queue))
        {
            return DefaultErrors.QueueDoesNotExist;
        }
        
        if (queue.TryEnqueue(Key, Payload))
        {
            return EnqueueResult.Ok;
        }
        
        return EnqueueResult.Full;
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

        queue.TryEnqueue(Key, Payload);
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