using Raft.Core.Commands;
using Serilog;

namespace Raft.Core.StateMachine;

public class RaftStateMachine: INodeState
{
    private readonly Node _node;
    private readonly ILogger _logger;
    public INodeState CurrentState { get; internal set; }
    internal ITimer ElectionTimer { get; }
    internal ITimer HeartbeatTimer { get; }

    public RaftStateMachine(Node node, ILogger logger, ITimer electionTimer, ITimer heartbeatTimer)
    {
        _node = node;
        _logger = logger;
        ElectionTimer = electionTimer;
        HeartbeatTimer = heartbeatTimer;
        GoToFollowerState();
    }
    

    public Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token)
    {
        return CurrentState.Apply(request, token);
    }

    public Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token)
    {
        return CurrentState.Apply(request, token);
    }

    internal void GoToFollowerState()
    {
        var state = new FollowerState(_node, this, _logger);
        ElectionTimer.Timeout += ElectionTimeoutCallback;
        CurrentState = state;
        ElectionTimer.Start();
    }

    internal void GoToCandidateState()
    {
        CurrentState = new CandidateState();
        ElectionTimer.Stop();
        HeartbeatTimer.Stop();
    }

    private void ElectionTimeoutCallback()
    {
        ElectionTimer.Timeout -= ElectionTimeoutCallback;
        GoToCandidateState();
    }
}