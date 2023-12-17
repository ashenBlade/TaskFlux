using TaskFlux.Commands.Visitors;
using TaskFlux.Models;
using TaskFlux.Serialization;

namespace TaskFlux.Commands.DeleteQueue;

public class DeleteQueueResponse : Response
{
    public override ResponseType Type => ResponseType.DeleteQueue;
    public QueueName QueueName { get; }

    public DeleteQueueResponse(QueueName queueName)
    {
        QueueName = queueName;
    }

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override bool TryGetDelta(out Delta delta)
    {
        delta = new DeleteQueueDelta(QueueName);
        return true;
    }

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}