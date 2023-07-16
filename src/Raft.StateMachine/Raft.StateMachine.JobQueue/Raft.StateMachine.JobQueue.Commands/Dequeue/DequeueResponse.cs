namespace Raft.StateMachine.JobQueue.Commands.Dequeue;

public record DequeueResponse(bool Success, int Key, byte[] Payload): IJobQueueResponse
{
    public ResponseType Type => ResponseType.Dequeue;
    
    public static DequeueResponse Ok(int key, byte[] payload) => new(true, key, payload);
    public static readonly DequeueResponse Empty = new(false, 0, Array.Empty<byte>());
    public void Accept(IJobQueueResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}

