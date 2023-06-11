using Raft.Core.Commands;
using Serilog;

namespace Raft.Core.StateMachine;

public class FollowerState: INodeState
{
    private readonly Node _node;
    private readonly RaftStateMachine _stateMachine;
    private readonly ILogger _logger;

    private FollowerState(RaftStateMachine stateMachine, ILogger logger)
    {
        _node = stateMachine.Node;
        _stateMachine = stateMachine;
        _logger = logger;
        _stateMachine.ElectionTimer.Timeout += OnElectionTimerTimeout;
    }
    
    public async Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token)
    {
        // Мы в более актуальном Term'е
        if (request.CandidateTerm < _node.CurrentTerm)
        {
            return new RequestVoteResponse()
            {
                CurrentTerm = _node.CurrentTerm,
                VoteGranted = false
            };
        }

        var canVote = 
            // Ранее не голосовали
            _node.VotedFor is null || 
            // Текущий лидер посылает этот запрос (почему бы не согласиться)
            _node.VotedFor == request.CandidateId;
        
        // Отдать свободный голос можем только за кандидата с более актуальным логом
        if (canVote && _node.LastLogEntry.IsUpToDateWith(request.LastLog))
        {
            // Проголосуем за кандидата - обновим состояние узла
            _node.CurrentTerm = request.CandidateTerm;
            _node.VotedFor = request.CandidateId;
            
            // И подтвердим свой 
            return new RequestVoteResponse()
            {
                CurrentTerm = _node.CurrentTerm,
                VoteGranted = true,
            };
        }
        
        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse()
        {
            CurrentTerm = _node.CurrentTerm, 
            VoteGranted = false
        };
    }

    public async Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token)
    {
        _stateMachine.ElectionTimer.Reset();
        _logger.Verbose("Получен Heartbeat");
        return new HeartbeatResponse();
    }

    public static FollowerState Start(RaftStateMachine stateMachine)
    {
        var state = new FollowerState(stateMachine, Log.ForContext("SourceContext", "Follower"));
        stateMachine.ElectionTimer.Start();
        return state;
    }

    private void OnElectionTimerTimeout()
    {
        _stateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        _stateMachine.ElectionTimer.Stop();
        _stateMachine.CurrentState = CandidateState.Start(_stateMachine); 
    }
    
    public void Dispose()
    {
        _stateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
    }
}