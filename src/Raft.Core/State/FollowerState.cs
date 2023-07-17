using Raft.Core.Commands;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Commands.Submit;
using Serilog;

namespace Raft.Core.State;

internal class FollowerState: BaseConsensusModuleState
{
    public override NodeRole Role => NodeRole.Follower;
    private readonly ILogger _logger;

    private FollowerState(IConsensusModule consensusModule, ILogger logger)
        : base(consensusModule)
    {
        _logger = logger;
        ElectionTimer.Timeout += OnElectionTimerTimeout;
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        _logger.Verbose("Получен RequestVote");
        ElectionTimer.Reset();
        
        // Мы в более актуальном Term'е
        if (request.CandidateTerm < CurrentTerm)
        {
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
        }

        if (CurrentTerm < request.CandidateTerm)
        {
            _logger.Debug("Получен RequestVote с большим термом {MyTerm} < {NewTerm}. Перехожу в Follower", CurrentTerm, request.CandidateTerm);
            ConsensusModule.UpdateState(request.CandidateTerm, request.CandidateId);

            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }
        
        var canVote = 
            // Ранее не голосовали
            VotedFor is null || 
            // Текущий лидер/кандидат посылает этот запрос (почему бы не согласиться)
            VotedFor == request.CandidateId;
        
        // Отдать свободный голос можем только за кандидата 
        if (canVote && 
            // У которого лог в консистентном с нашим состоянием
            !Log.Conflicts(request.LastLogEntryInfo))
        {
            // CurrentTerm = request.CandidateTerm;
            // VotedFor = request.CandidateId;
            ConsensusModule.UpdateState(request.CandidateTerm, request.CandidateId);
            
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }
        
        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        ElectionTimer.Reset();
        if (request.Term < CurrentTerm)
        {
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        if (CurrentTerm < request.Term)
        {
            ConsensusModule.UpdateState(request.Term, null);
        }
        
        if (Log.Contains(request.PrevLogEntryInfo) is false)
        {
            return AppendEntriesResponse.Fail(CurrentTerm);
        }
        
        if (0 < request.Entries.Count)
        {
            Log.InsertRange(request.Entries, request.PrevLogEntryInfo.Index + 1);
        }
        
        if (Log.CommitIndex < request.LeaderCommit)
        {
            Log.Commit(Math.Min(request.LeaderCommit, Log.LastEntry.Index));
            Log.ApplyCommitted(StateMachine);
        }

        return AppendEntriesResponse.Ok(CurrentTerm);
    }

    public override SubmitResponse Apply(SubmitRequest request)
    {
        return SubmitResponse.NotALeader;
    }

    internal static FollowerState Create(IConsensusModule consensusModule)
    {
        return new FollowerState(consensusModule, consensusModule.Logger.ForContext("SourceContext", "Follower"));
    }

    private void OnElectionTimerTimeout()
    {
        _logger.Debug("Сработал Election Timeout. Перехожу в состояние Candidate");
        CommandQueue.Enqueue(new MoveToCandidateAfterElectionTimerTimeoutCommand(this, ConsensusModule));
    }
    
    public override void Dispose()
    {
        ElectionTimer.Timeout -= OnElectionTimerTimeout;
    }
}