namespace TaskFlux.Requests.Dequeue;

public record DequeueResponse(bool Success, int Key, byte[] Payload): IResponse
{
    public static DequeueResponse Ok(int key, byte[] payload) => new(true, key, payload);
    public static readonly DequeueResponse Empty = new(false, 0, Array.Empty<byte>());
    public void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}

