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
        Task.WaitAll(Node.PeerGroup.Peers.Select(x => x.SendHeartbeat(request, CancellationToken.None)).ToArray());
        _logger.Verbose("Heartbeat отправлены. Перезапускаю таймер");
        StateMachine.HeartbeatTimer.Start();
    }

    public override void Dispose()
    {
        lock (UpdateLocker)
        {
            StateMachine.HeartbeatTimer.Stop();
            StateMachine.HeartbeatTimer.Timeout -= SendHeartbeat;
        }
        base.Dispose();
    }

    public static LeaderState Create(IStateMachine stateMachine)
    {
        return new LeaderState(stateMachine, stateMachine.Logger.ForContext("SourceContext", "Leader"));
    }
}