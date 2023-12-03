using TaskFlux.Commands.Visitors;
using TaskFlux.Models;
using TaskFlux.Serialization;

namespace TaskFlux.Commands.Enqueue;

public class EnqueueResponse : Response
{
    public EnqueueResponse(QueueName queueName, long key, byte[] message)
    {
        QueueName = queueName;
        Key = key;
        Message = message;
    }

    public override ResponseType Type => ResponseType.Enqueue;
    public QueueName QueueName { get; }
    public long Key { get; }
    public byte[] Message { get; }

    public override void Accept(IResponseVisitor visitor)
    {
        throw new NotImplementedException();
    }

    public override bool TryGetDelta(out Delta delta)
    {
        delta = new AddRecordDelta(QueueName, Key, Message);
        return true;
    }

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}