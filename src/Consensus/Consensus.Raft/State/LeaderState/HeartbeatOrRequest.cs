namespace Consensus.Raft.State.LeaderState;

internal record struct HeartbeatOrRequest
{
    private readonly LogReplicationRequest? _request;
    private readonly HeartbeatSynchronizer? _heartbeatSynchronizer;

    private HeartbeatOrRequest(LogReplicationRequest? request, HeartbeatSynchronizer? heartbeatSynchronizer)
    {
        _request = request;
        _heartbeatSynchronizer = heartbeatSynchronizer;
    }

    public bool TryGetRequest(out LogReplicationRequest request)
    {
        if (_request is { } r)
        {
            request = r;
            return true;
        }

        request = default!;
        return false;
    }

    public bool TryGetHeartbeat(out HeartbeatSynchronizer synchronizer)
    {
        if (_heartbeatSynchronizer is { } r)
        {
            synchronizer = r;
            return true;
        }

        synchronizer = default!;
        return false;
    }

    public static HeartbeatOrRequest Heartbeat(HeartbeatSynchronizer synchronizer) => new(null, synchronizer);
    public static HeartbeatOrRequest Request(LogReplicationRequest request) => new(request, null);
}