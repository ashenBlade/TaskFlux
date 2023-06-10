using Raft.Core.Commands;
using Serilog;

namespace Raft.Core.StateMachine;

public class LeaderState: INodeState
{
    private readonly RaftStateMachine _stateMachine;
    private readonly ILogger _logger;

    public LeaderState(RaftStateMachine stateMachine, ILogger logger)
    {
        _stateMachine = stateMachine;
        _logger = logger;
        _stateMachine.HeartbeatTimer.Timeout += SendHeartbeat;
    }

    public void SendHeartbeat()
    {
        var request = new HeartbeatRequest()
        { };
        Task.WhenAll(_stateMachine.Node.Peers.Select(x => x.SendHeartbeat(request, CancellationToken.None)));
    }
    
    public Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    public Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token)
    {
        throw new NotImplementedException();
    }
}