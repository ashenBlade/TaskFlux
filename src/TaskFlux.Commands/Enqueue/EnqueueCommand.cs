using System.Text.Json.Serialization;
using TaskFlux.Core;

namespace TaskFlux.Commands.Enqueue;

public class EnqueueCommand: Command
{
    public int Key { get; }
    public byte[] Payload { get; }

    public EnqueueCommand(int key, byte[] payload)
    {
        Key = key;
        Payload = payload;
    }
    public override CommandType Type => CommandType.Enqueue;
    public override Result Apply(INode node)
    {
        var queue = node.GetJobQueue();
        if (queue.TryEnqueue(Key, Payload))
        {
            return EnqueueResult.Ok;
        }
        
        return EnqueueResult.Fail;
    }

    public override void ApplyNoResult(INode node)
    {
        node.GetJobQueue()
            .TryEnqueue(Key, Payload);
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