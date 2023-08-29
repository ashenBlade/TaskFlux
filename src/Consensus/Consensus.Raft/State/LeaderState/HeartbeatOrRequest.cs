namespace Consensus.Raft.State.LeaderState;

internal record struct HeartbeatOrRequest
{
    private readonly LogReplicationRequest? _request;

    private HeartbeatOrRequest(LogReplicationRequest? request)
    {
        _request = request;
    }

    public bool TryGetRequest(out LogReplicationRequest synchronizer)
    {
        if (_request is { } s)
        {
            synchronizer = s;
            return true;
        }

        synchronizer = default!;
        return false;
    }

    public static HeartbeatOrRequest Heartbeat => new();
    public static HeartbeatOrRequest Request(LogReplicationRequest? request) => new(request);
}