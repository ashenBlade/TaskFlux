using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Peer;
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
        var request = new HeartbeatRequest(
            Term: Node.CurrentTerm, 
            LeaderCommit: Log.CommitIndex,
            LeaderId: Node.Id,
            PrevLogEntry: Log.LastLogEntry);
        
        var tasks = Node.PeerGroup.Peers.Select<IPeer, Task<Term?>>(async peer =>
        {
            var response = await peer.SendHeartbeat(request, CancellationToken.None);
            if (response is null or {Success: true})
            {
                return null;
            }

            if (Node.CurrentTerm < response.Term)
            {
                return response.Term;
            }

            return null;
        }).ToArray();
        Task.WaitAll(tasks);
        
        Term? maxTerm = null;
        
        foreach (var task in tasks)
        {
            if (task is {IsCompletedSuccessfully: true, Result: {} term})
            {
                if (maxTerm is null)
                {
                    maxTerm = term;
                }
                else if (maxTerm.Value < term)
                {
                    maxTerm = term;
                }
            }
        }

        if (maxTerm is {} max && Node.CurrentTerm < max)
        {
            _logger.Debug("Какой-то узел ответил большим термом - {Term}. Перехожу в Follower", max);
            StateMachine.CommandQueue.Enqueue(new MoveToFollowerStateCommand(max, null, this, StateMachine));
        }
        else
        {
            _logger.Verbose("Heartbeat отправлены. Посылаю команду на перезапуск таймера");
            StateMachine.CommandQueue.Enqueue(new StartHeartbeatTimerCommand(this, StateMachine));
        }
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