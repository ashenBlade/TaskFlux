using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Serilog;

namespace Raft.Core.StateMachine;

internal class LeaderState: BaseState
{
    public override NodeRole Role => NodeRole.Leader;
    private readonly ILogger _logger;

    internal LeaderState(IStateMachine stateMachine, ILogger logger)
        : base(stateMachine)
    {
        _logger = logger;
        StateMachine.HeartbeatTimer.Timeout += SendHeartbeat;
    }

    // ReSharper disable once CoVariantArrayConversion
    private void SendHeartbeat()
    {
        _logger.Verbose("Отправляю Heartbeat");
        var request = new HeartbeatRequest();
        Task.WaitAll(StateMachine.Node.PeerGroup.Peers.Select(x => x.SendHeartbeat(request, CancellationToken.None)).ToArray());
        StateMachine.HeartbeatTimer.Start();
    }
    
    public override async Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token = default)
    {
        return await base.Apply(request, token);
    }

    public override async Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token = default)
    {
        var response = await base.Apply(request, token);
        return response;
    }

    public override void Dispose()
    {
        StateMachine.HeartbeatTimer.Stop();
        StateMachine.HeartbeatTimer.Timeout -= SendHeartbeat;
        base.Dispose();
    }

    public static LeaderState Start(IStateMachine stateMachine)
    {
        var state = new LeaderState(stateMachine, stateMachine.Logger.ForContext("SourceContext", "Leader"));
        stateMachine.HeartbeatTimer.Start();
        return state;
    }
}