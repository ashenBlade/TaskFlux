namespace TaskFlux.Commands.Dequeue;

public class DequeueResult: Result
{
    public static readonly DequeueResult Empty = new(false, 0, null);
    public static DequeueResult Create(int key, byte[] payload) => new(true, key, payload);
    
    public bool Success { get; }
    public int Key { get; }
    public byte[] Payload { get; }

    internal DequeueResult(bool success, int key, byte[]? payload)
    {
        Success = success;
        Key = key;
        Payload = payload ?? Array.Empty<byte>();
    }
    
    public bool TryGetResult(out int key, out byte[] payload)
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


    public override ResultType Type => ResultType.Dequeue;
    public override void Accept(IResultVisitor visitor)
    {
        visitor.Visit(this);
    }
    
    
    public override ValueTask AcceptAsync(IAsyncResultVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}