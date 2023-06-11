using Raft.Core.Commands;
using Serilog;

namespace Raft.Core.StateMachine;

public class RaftStateMachine: IDisposable
{
    internal readonly ILogger Logger;
    internal Node Node { get; }
    private INodeState _currentState;
    internal INodeState CurrentState
    {
        get => _currentState;
        set
        {
            _currentState?.Dispose();
            _currentState = value;
        }
    }

    internal ITimer ElectionTimer { get; }
    internal ITimer HeartbeatTimer { get; }

    public RaftStateMachine(Node node, ILogger logger, ITimer electionTimer, ITimer heartbeatTimer)
    {
        Node = node;
        Logger = logger;
        ElectionTimer = electionTimer;
        HeartbeatTimer = heartbeatTimer;
        logger.Debug("Перехожу в состояние Follower");
        _currentState = FollowerState.Start(this);
    }
    

    public Task<RequestVoteResponse> Handle(RequestVoteRequest request, CancellationToken token)
    {
        return CurrentState.Apply(request, token);
    }

    public Task<HeartbeatResponse> Handle(HeartbeatRequest request, CancellationToken token)
    {
        return CurrentState.Apply(request, token);
    }
    public void Dispose()
    {
        _currentState.Dispose();
    }
}