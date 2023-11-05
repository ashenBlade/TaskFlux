using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.Dequeue;

public class DequeueResponse : Response
{
    public static readonly DequeueResponse Empty = new(false, 0, null);
    public static DequeueResponse Create(long key, byte[] payload) => new(true, key, payload);

    public bool Success { get; }
    public long Key { get; }
    public byte[] Payload { get; }

    internal DequeueResponse(bool success, long key, byte[]? payload)
    {
        Success = success;
        Key = key;
        Payload = payload ?? Array.Empty<byte>();
    }

    public bool TryGetResult(out long key, out byte[] payload)
    {
        if (Success)
        {
            key = Key;
            payload = Payload;
            return true;
        }

        key = 0;
        payload = Array.Empty<byte>();
        return false;
    }


    public override ResponseType Type => ResponseType.Dequeue;

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncResponseVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}