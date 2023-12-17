using TaskFlux.Commands.Visitors;
using TaskFlux.Models;
using TaskFlux.PriorityQueue;
using TaskFlux.Serialization;

namespace TaskFlux.Commands.CreateQueue;

public class CreateQueueResponse : Response
{
    public override ResponseType Type => ResponseType.CreateQueue;
    public QueueName QueueName { get; }
    public PriorityQueueCode Code { get; }
    public int? MaxQueueSize { get; }
    public int? MaxMessageSize { get; }
    public (long, long)? PriorityRange { get; }

    public CreateQueueResponse(QueueName queueName,
                               PriorityQueueCode code,
                               int? maxQueueSize,
                               int? maxMessageSize,
                               (long, long)? priorityRange)
    {
        QueueName = queueName;
        Code = code;
        MaxQueueSize = maxQueueSize;
        MaxMessageSize = maxMessageSize;
        PriorityRange = priorityRange;
    }

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override bool TryGetDelta(out Delta delta)
    {
        delta = new CreateQueueDelta(QueueName, Code, MaxQueueSize, MaxMessageSize, PriorityRange);
        return true;
    }

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}