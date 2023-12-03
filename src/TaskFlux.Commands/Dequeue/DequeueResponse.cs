using TaskFlux.Commands.Visitors;
using TaskFlux.Models;
using TaskFlux.Serialization;

namespace TaskFlux.Commands.Dequeue;

public class DequeueResponse : Response
{
    public static readonly DequeueResponse Empty = new(false, QueueName.Default, 0, null);

    public static DequeueResponse Create(QueueName queueName, long key, byte[] payload) =>
        new(true, queueName, key, payload);

    public override ResponseType Type => ResponseType.Dequeue;

    public bool Success { get; }
    public QueueName QueueName { get; }
    public long Key { get; }
    public byte[] Message { get; }

    internal DequeueResponse(bool success, QueueName queueName, long key, byte[]? payload)
    {
        Success = success;
        QueueName = queueName;
        Key = key;
        Message = payload ?? Array.Empty<byte>();
    }

    public bool TryGetResult(out long key, out byte[] payload)
    {
        if (Success)
        {
            key = Key;
            payload = Message;
            return true;
        }

        key = 0;
        payload = Array.Empty<byte>();
        return false;
    }

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override bool TryGetDelta(out Delta delta)
    {
        if (!Success)
        {
            delta = default!;
            return false;
        }


        delta = new RemoveRecordDelta(QueueName, Key, Message);
        return true;
    }

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}