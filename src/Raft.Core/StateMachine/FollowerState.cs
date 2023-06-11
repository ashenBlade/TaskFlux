using Raft.Core.Commands;
using Serilog;

namespace Raft.Core.StateMachine;

public class FollowerState: INodeState
{
    private Node Node => _stateMachine.Node;
    private readonly RaftStateMachine _stateMachine;
    private readonly ILogger _logger;

    private FollowerState(RaftStateMachine stateMachine, ILogger logger)
    {
        _stateMachine = stateMachine;
        _logger = logger;
        _stateMachine.ElectionTimer.Timeout += OnElectionTimerTimeout;
    }
    
    public async Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token)
    {
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
            // Текущий лидер посылает этот запрос (почему бы не согласиться)
            Node.VotedFor == request.CandidateId;
        
        // Отдать свободный голос можем только за кандидата с более актуальным логом
        if (canVote && Node.LastLogEntry.IsUpToDateWith(request.LastLog))
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

    public async Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token)
    {
        _stateMachine.ElectionTimer.Reset();
        _logger.Verbose("Получен Heartbeat");
        return new HeartbeatResponse();
    }

    public static FollowerState Start(RaftStateMachine stateMachine)
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