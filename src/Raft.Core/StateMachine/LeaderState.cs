using Raft.Core.Commands;
using Serilog;

namespace Raft.Core.StateMachine;

internal class LeaderState: INodeState
{
    public NodeRole Role => NodeRole.Leader;
    private readonly IStateMachine _stateMachine;
    private readonly ILogger _logger;
    
    private LeaderState(IStateMachine stateMachine, ILogger logger)
    {
        _stateMachine = stateMachine;
        _logger = logger;
        _stateMachine.HeartbeatTimer.Timeout += SendHeartbeat;
    }

    // ReSharper disable once CoVariantArrayConversion
    private void SendHeartbeat()
    {
        _logger.Verbose("Отправляю Heartbeat");
        var request = new HeartbeatRequest();
        Task.WaitAll(_stateMachine.Node.PeerGroup.Peers.Select(x => x.SendHeartbeat(request, CancellationToken.None)).ToArray());
        _stateMachine.HeartbeatTimer.Start();
    }
    
    public Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    public Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
        _stateMachine.HeartbeatTimer.Timeout -= SendHeartbeat;
    }

    public static LeaderState Start(IStateMachine stateMachine)
    {
        var state = new LeaderState(stateMachine, stateMachine.Logger.ForContext("SourceContext", "Leader"));
        stateMachine.HeartbeatTimer.Start();
        return state;
    }
}