namespace Raft.StateMachine.JobQueue.Requests;

public record EnqueueRequest(int Key, byte[] Payload);

public record EnqueueResponse(bool Success)
{
    public static readonly EnqueueResponse Ok = new(true);
    public static readonly EnqueueResponse Fail = new(false);
}