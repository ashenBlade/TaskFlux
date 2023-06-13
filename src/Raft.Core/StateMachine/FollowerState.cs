using System.ComponentModel;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.StateMachine;

internal class FollowerState: NodeState
{
    public override NodeRole Role => NodeRole.Follower;
    private readonly ILogger _logger;

    internal FollowerState(IStateMachine stateMachine, ILogger logger)
        : base(stateMachine)
    {
        _logger = logger;
        StateMachine.ElectionTimer.Timeout += OnElectionTimerTimeout;
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request, CancellationToken token = default)
    {
        StateMachine.ElectionTimer.Reset();
        return base.Apply(request, token);
    }

    public override HeartbeatResponse Apply(HeartbeatRequest request, CancellationToken token = default)
    {
        StateMachine.ElectionTimer.Reset();
        _logger.Verbose("Получен Heartbeat");
        return base.Apply(request, token);
    }

    internal static FollowerState Create(IStateMachine stateMachine)
    {
        return new FollowerState(stateMachine, stateMachine.Logger.ForContext("SourceContext", "Follower"));
    }

    private void OnElectionTimerTimeout()
    {
        lock (UpdateLocker)
        {
            if (StateMachine.CurrentState != this)
            {
                _logger.Verbose("Election timeout сработал, но после получения блокировки состояние было обновлено");
                return;
            }
            
            StateMachine.ElectionTimer.Stop();
            _logger.Debug("Сработал Election Timeout. Перехожу в состояние Candidate");
            StateMachine.CurrentState = CandidateState.Create(StateMachine);
            Node.CurrentTerm = Node.CurrentTerm.Increment();
            Node.VotedFor = Node.Id;
            StateMachine.ElectionTimer.Start();
        }
    }
    
    public override void Dispose()
    {
        StateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        base.Dispose();
    }
}