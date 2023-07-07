namespace Raft.StateMachine.JobQueue.Requests;

public record GetCountRequest()
{
    public static readonly GetCountRequest Instance = new();
}

public record GetCountResponse(int Count)
{
    public static readonly GetCountResponse Empty = new(0);
}
