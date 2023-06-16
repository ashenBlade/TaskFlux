using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Serilog;

namespace Raft.Core.StateMachine;

internal class LeaderState: NodeState
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
        var request = new HeartbeatRequest()
        {
            Term = Node.CurrentTerm,
            LeaderCommit = Log.CommitIndex,
            LeaderId = Node.Id,
            PrevLogEntry = Log.LastLogEntry
        };
        Task.WaitAll(Node.PeerGroup.Peers.Select(async peer =>
        {
            var response = await peer.SendHeartbeat(request, CancellationToken.None);
            if (response is null or {Success: true})
            {
                return;
            }

            if (response.Term < Node.CurrentTerm)
            {
                StateMachine.CommandQueue.Enqueue(new MoveToFollowerStateCommand(response.Term, null, this,
                    StateMachine));
            }
        }).ToArray());
        _logger.Verbose("Heartbeat отправлены. Посылаю команду на перезапуск таймера");
        StateMachine.CommandQueue.Enqueue(new StartHeartbeatTimerCommand(this, StateMachine));
    }

    public override void Dispose()
    {
        StateMachine.CommandQueue.Enqueue(new StopHeartbeatTimerCommand(this, StateMachine));
        StateMachine.HeartbeatTimer.Timeout -= SendHeartbeat;
        base.Dispose();
    }

    public static LeaderState Create(IStateMachine stateMachine)
    {
        return new LeaderState(stateMachine, stateMachine.Logger.ForContext("SourceContext", "Leader"));
    }
}