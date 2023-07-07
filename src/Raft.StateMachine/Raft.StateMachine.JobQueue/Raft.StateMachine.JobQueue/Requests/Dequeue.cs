namespace Raft.StateMachine.JobQueue.Requests;

public record DequeueRequest()
{
    public static readonly DequeueRequest Instance = new();
}

public record DequeueResponse(bool Success, int Key, byte[] Payload)
{
    public static DequeueResponse Ok(int key, byte[] payload) => new(true, key, payload);
    public static readonly DequeueResponse Empty = new(false, 0, Array.Empty<byte>());
}

