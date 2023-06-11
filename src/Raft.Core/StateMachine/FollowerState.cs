using Raft.Core.Commands;
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
            Node.LastLogEntry.IsUpToDateWith(request.LastLog))
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
        return new HeartbeatResponse();
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