using System.Diagnostics;
using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Core.Subscription;

namespace TaskFlux.Core.Commands.Dequeue;

public class QueueSubscriberResponse : Response
{
    public override ResponseType Type => ResponseType.Subscription;

    /// <summary>
    /// Очередь, на которую мы подписываемся
    /// </summary>
    public QueueName Queue { get; set; }

    /// <summary>
    /// Таймаут ожидания, который следует использовать
    /// </summary>
    public TimeSpan Timeout { get; }

    /// <summary>
    /// Подписчик, который должен получить новую запись
    /// </summary>
    public IQueueSubscriber Subscriber { get; }

    public QueueSubscriberResponse(IQueueSubscriber subscriber, QueueName queue, TimeSpan timeout)
    {
        Debug.Assert(subscriber is not null, "subscriber is not null");

        Subscriber = subscriber;
        Queue = queue;
        Timeout = timeout;
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