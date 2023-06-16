using System.ComponentModel;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.StateMachine;

internal class  FollowerState: NodeState
{
    public override NodeRole Role => NodeRole.Follower;
    private readonly ILogger _logger;

    internal FollowerState(IStateMachine stateMachine, ILogger logger)
        : base(stateMachine)
    {
        _logger = logger;
        StateMachine.ElectionTimer.Timeout += OnElectionTimerTimeout;
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        _logger.Verbose("Получен RequestVote");
        StateMachine.CommandQueue.Enqueue(new ResetElectionTimerCommand(this, StateMachine));
        return base.Apply(request);
    }

    public override HeartbeatResponse Apply(HeartbeatRequest request)
    {
        _logger.Verbose("Получен Heartbeat");
        StateMachine.CommandQueue.Enqueue(new ResetElectionTimerCommand(this, StateMachine));
        return base.Apply(request);
    }

    internal static FollowerState Create(IStateMachine stateMachine)
    {
        return new FollowerState(stateMachine, stateMachine.Logger.ForContext("SourceContext", "Follower"));
    }

    private void OnElectionTimerTimeout()
    {
        _logger.Debug("Сработал Election Timeout. Перехожу в состояние Candidate");
        StateMachine.CommandQueue.Enqueue(new MoveToCandidateAfterElectionTimerTimeoutCommand(this, StateMachine));
    }
    
    public override void Dispose()
    {
        StateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        base.Dispose();
    }
}