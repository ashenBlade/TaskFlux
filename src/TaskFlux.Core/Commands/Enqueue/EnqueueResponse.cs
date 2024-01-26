using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Enqueue;

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
        visitor.Visit(this);
    }

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}