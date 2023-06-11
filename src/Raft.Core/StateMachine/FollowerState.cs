using System.ComponentModel;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.StateMachine;

public class FollowerState: INodeState
{
    public NodeRole Role => NodeRole.Follower;
    private INode Node => _stateMachine.Node;
    private readonly IStateMachine _stateMachine;
    private readonly ILogger _logger;

    internal FollowerState(IStateMachine stateMachine, ILogger logger)
    {
        _stateMachine = stateMachine;
        _logger = logger;
        _stateMachine.ElectionTimer.Timeout += OnElectionTimerTimeout;
    }
    
    public async Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token = default)
    {
        _stateMachine.ElectionTimer.Reset();
        
        // Мы в более актуальном Term'е
        if (request.CandidateTerm < Node.CurrentTerm)
        {
            return new RequestVoteResponse()
            {
                CurrentTerm = Node.CurrentTerm,
                VoteGranted = false
            };
        }

        var canVote = 
            // Ранее не голосовали
            Node.VotedFor is null || 
            // Текущий лидер/кандидат посылает этот запрос (почему бы не согласиться)
            Node.VotedFor == request.CandidateId;
        
        // Отдать свободный голос можем только за кандидата 
        if (canVote && 
            // С термом больше нашего (иначе, на текущем терме уже есть лидер)
            Node.CurrentTerm < request.CandidateTerm && 
            // У которого лог не "младше" нашего
            _stateMachine.Log.LastLogEntry.IsUpToDateWith(request.LastLog))
        {
            // Проголосуем за кандидата - обновим состояние узла
            Node.CurrentTerm = request.CandidateTerm;
            Node.VotedFor = request.CandidateId;
            
            // И подтвердим свой 
            return new RequestVoteResponse()
            {
                CurrentTerm = Node.CurrentTerm,
                VoteGranted = true,
            };
        }
        
        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse()
        {
            CurrentTerm = Node.CurrentTerm, 
            VoteGranted = false
        };
    }

    public async Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token = default)
    {
        _stateMachine.ElectionTimer.Reset();
        _logger.Verbose("Получен Heartbeat");
        if (request.Term < Node.CurrentTerm)
        {
            return HeartbeatResponse.Fail(Node.CurrentTerm);
        }

        LogEntryCheckResult checkResult;
        switch (checkResult = _stateMachine.Log.Check(request.PrevLogEntry))
        {
            case LogEntryCheckResult.Conflict:
                return HeartbeatResponse.Fail(Node.CurrentTerm);
            case LogEntryCheckResult.Contains:
            case LogEntryCheckResult.NotFound:
                // TODO: отправить индекс последней закомиченной записи
                break;
            default:
                throw new InvalidEnumArgumentException(nameof(LogEntryCheckResult), (int)checkResult, typeof(LogEntryCheckResult));
        }

        
        if (Node.CommitIndex < request.LeaderCommit)
        {
            Node.CommitIndex = Math.Min(request.LeaderCommit, _stateMachine.Log.LastLogEntry.Index);
        }

        if (Node.CurrentTerm < request.Term)
        {
            Node.CurrentTerm = request.Term;
        }

        return HeartbeatResponse.Ok(Node.CurrentTerm);
    }

    internal static FollowerState Start(IStateMachine stateMachine)
    {
        var logger = stateMachine.Logger.ForContext("SourceContext", "Follower");
        var state = new FollowerState(stateMachine, logger);
        stateMachine.ElectionTimer.Start();
        stateMachine.Node.VotedFor = null;
        return state;
    }

    private void OnElectionTimerTimeout()
    {
        _stateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        _stateMachine.ElectionTimer.Stop();
        _logger.Debug("Сработал Election Timeout. Перехожу в состояние Candidate");
        _stateMachine.CurrentState = CandidateState.Start(_stateMachine); 
    }
    
    public void Dispose()
    {
        _stateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
    }
}