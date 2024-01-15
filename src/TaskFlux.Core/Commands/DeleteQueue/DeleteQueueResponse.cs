using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.DeleteQueue;

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

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}