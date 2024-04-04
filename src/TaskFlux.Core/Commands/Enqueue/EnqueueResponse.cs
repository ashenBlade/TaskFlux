using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Core.Queue;

namespace TaskFlux.Core.Commands.Enqueue;

public class EnqueueResponse : Response
{
    public override ResponseType Type => ResponseType.Enqueue;

    public EnqueueResponse(QueueName queueName, QueueRecord record)
    {
        QueueName = queueName;
        Record = record;
    }

    public QueueName QueueName { get; }
    public QueueRecord Record { get; }
    public long Priority => Record.Priority;
    public byte[] Payload => Record.Payload;
    public RecordId Id => Record.Id;

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}