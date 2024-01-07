using Consensus.Core.Submit;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Serilog;
using TaskFlux.Models;

namespace Consensus.Raft.State;

public class CandidateState<TCommand, TResponse>
    : State<TCommand, TResponse>
{
    public override NodeRole Role => NodeRole.Candidate;

    /// <summary>
    /// Источник токенов времени жизни состояния
    /// </summary>
    private readonly CancellationTokenSource _lifetimeCts = new();

    // Когда мы кандидат, то еще не известно, кто лидер
    public override NodeId? LeaderId => null;
    private readonly ITimer _electionTimer;
    private readonly ILogger _logger;

    internal CandidateState(IRaftConsensusModule<TCommand, TResponse> raftConsensusModule,
                            ITimer electionTimer,
                            ILogger logger)
        : base(raftConsensusModule)
    {
        _electionTimer = electionTimer;
        _logger = logger;
    }

    public override void Initialize()
    {
        // В будущем, для кластера из одного узла сделаю отдельную конфигурацию при старте,
        // а сейчас предполагаем, что есть и другие узлы и нужно запустить их обработчиков

        var term = CurrentTerm;
        var nodeId = Id;
        var lastEntry = PersistenceFacade.LastEntry;
        var coordinator = new ElectionCoordinator<TCommand, TResponse>(this, _logger, term);

        _logger.Information("Запускаю обработчиков для сбора консенсуса");
        foreach (var peer in PeerGroup.Peers)
        {
            var worker =
                new PeerElectorBackgroundJob<TCommand, TResponse>(term, lastEntry, nodeId, peer, coordinator, _logger);
            BackgroundJobQueue.Accept(worker, _lifetimeCts.Token);
        }

        _electionTimer.Timeout += OnElectionTimerTimeout;
        _electionTimer.Schedule();
    }

    private void OnElectionTimerTimeout()
    {
        _electionTimer.Timeout -= OnElectionTimerTimeout;

        // Пока такой хак, думаю в будущем, если буду один в кластере - сразу лидером стартую
        if (PeerGroup.Peers.Count == 0)
        {
            var leaderState = RaftConsensusModule.CreateLeaderState();
            if (RaftConsensusModule.TryUpdateState(leaderState, this))
            {
                _logger.Debug("Сработал Election Timeout и в кластере я один. Становлюсь лидером");
                PersistenceFacade.UpdateState(CurrentTerm.Increment(), null);
                return;
            }
        }

        var candidateState = RaftConsensusModule.CreateCandidateState();
        if (RaftConsensusModule.TryUpdateState(candidateState, this))
        {
            _logger.Debug("Сработал Election Timeout. Перехожу в новый терм");
            PersistenceFacade.UpdateState(CurrentTerm.Increment(), Id);
        }
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        if (request.Term < CurrentTerm)
        {
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        var followerState = RaftConsensusModule.CreateFollowerState();
        RaftConsensusModule.TryUpdateState(followerState, this);
        return RaftConsensusModule.Handle(request);
    }

    public override SubmitResponse<TResponse> Apply(TCommand command, CancellationToken token = default)
    {
        return SubmitResponse<TResponse>.NotLeader;
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        // Мы в более актуальном Term'е
        if (request.CandidateTerm < CurrentTerm)
        {
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
        }

        var logConflicts = PersistenceFacade.Conflicts(request.LastLogEntryInfo);
        if (logConflicts)
        {
            _logger.Debug("При обработке RequestVote от узла {NodeId} обнаружен конфликт лога", request.CandidateId);
        }

        if (CurrentTerm < request.CandidateTerm)
        {
            var followerState = RaftConsensusModule.CreateFollowerState();
            if (RaftConsensusModule.TryUpdateState(followerState, this))
            {
                RaftConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, null);
            }

            return new RequestVoteResponse(CurrentTerm, !logConflicts);
        }

        var canVote =
            // Ранее не голосовали
            VotedFor is null
          ||
            // Текущий лидер/кандидат посылает этот запрос (почему бы не согласиться)
            VotedFor == request.CandidateId;

        // Отдать свободный голос можем только за кандидата 
        if (canVote
          &&
            // У которого лог в консистентном с нашим состоянием
            !logConflicts)
        {
            var followerState = RaftConsensusModule.CreateFollowerState();
            if (RaftConsensusModule.TryUpdateState(followerState, this))
            {
                RaftConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, null);
                return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
            }

            return RaftConsensusModule.Handle(request);
        }

        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
    }

    public override void Dispose()
    {
        _electionTimer.Timeout -= OnElectionTimerTimeout;
        _electionTimer.Stop();
        _lifetimeCts.Cancel();

        _lifetimeCts.Dispose();
        _electionTimer.Dispose();
    }

    public override InstallSnapshotResponse Apply(InstallSnapshotRequest request,
                                                  CancellationToken token = default)
    {
        if (request.Term < CurrentTerm)
        {
            return new InstallSnapshotResponse(CurrentTerm);
        }

        var state = RaftConsensusModule.CreateFollowerState();
        if (RaftConsensusModule.TryUpdateState(state, this))
        {
            _logger.Debug("Получен InstallSnapshotRequest с термом не меньше моего. Перехожу в Follower");
        }
        else
        {
            _logger.Debug("Получен InstallSnapshotRequest с термом не меньше моего, но перейти в Follower не удалось");
        }

        return RaftConsensusModule.Handle(request, token);
    }
}