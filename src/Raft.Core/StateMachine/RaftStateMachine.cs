using Raft.Core.Commands;
using Serilog;

namespace Raft.Core.StateMachine;

public class RaftStateMachine: IDisposable
{
    private readonly ILogger _logger;
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
        _logger = logger;
        ElectionTimer = electionTimer;
        HeartbeatTimer = heartbeatTimer;
        GoToFollowerState();
    }
    

    public Task<RequestVoteResponse> Handle(RequestVoteRequest request, CancellationToken token)
    {
        return CurrentState.Apply(request, token);
    }

    public Task<HeartbeatResponse> Handle(HeartbeatRequest request, CancellationToken token)
    {
        return CurrentState.Apply(request, token);
    }

    internal void GoToFollowerState()
    {
        CurrentState = FollowerState.Start(this);
        ElectionTimer.Start();
    }

    internal void GoToFollowerState(Term term)
    {
        var state = FollowerState.Start(this);
        Node.CurrentTerm = term;
        CurrentState = state;
        ElectionTimer.Start();
    }

    public void Dispose()
    {
        _currentState.Dispose();
    }
}