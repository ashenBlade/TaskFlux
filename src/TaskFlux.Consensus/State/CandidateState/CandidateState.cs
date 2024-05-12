using Serilog;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Core;

namespace TaskFlux.Consensus.State.CandidateState;

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

    internal CandidateState(RaftConsensusModule<TCommand, TResponse> consensusModule,
        ITimer electionTimer,
        ILogger logger)
        : base(consensusModule)
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
        var lastEntry = Persistence.LastEntry;
        var coordinator = new ElectionCoordinator<TCommand, TResponse>(this, _logger, term);

        CancellationToken token;
        try
        {
            token = _lifetimeCts.Token;
        }
        catch (ObjectDisposedException)
        {
            // Такое может случиться, когда мы только запустились в уже работающем кластере - лидер только сейчас отправил запрос и мы стали Follower
            return;
        }

        _logger.Information("Запускаю обработчиков для сбора консенсуса");
        foreach (var peer in PeerGroup.Peers)
        {
            var worker =
                new PeerElectorBackgroundJob<TCommand, TResponse>(term, lastEntry, nodeId, peer, coordinator, _logger);
            BackgroundJobQueue.Accept(worker, token);
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
            var leaderState = ConsensusModule.CreateLeaderState();
            if (ConsensusModule.TryUpdateState(leaderState, this))
            {
                _logger.Debug("Сработал Election Timeout и в кластере я один. Становлюсь лидером");
                Persistence.UpdateState(CurrentTerm.Increment(), Id);
                return;
            }
        }

        var candidateState = ConsensusModule.CreateCandidateState();
        if (ConsensusModule.TryUpdateState(candidateState, this))
        {
            _logger.Debug("Сработал Election Timeout. Перехожу в новый терм");
            Persistence.UpdateState(CurrentTerm.Increment(), Id);
        }
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        if (request.Term < CurrentTerm)
        {
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        var followerState = ConsensusModule.CreateFollowerState();
        ConsensusModule.TryUpdateState(followerState, this);
        return ConsensusModule.Handle(request);
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

        if (CurrentTerm < request.CandidateTerm)
        {
            // В любом случае обновляем терм и переходим в последователя
            var newTerm = request.CandidateTerm;
            var followerState = ConsensusModule.CreateFollowerState();
            if (ConsensusModule.TryUpdateState(followerState, this))
            {
                if (Persistence.IsUpToDate(request.LastLogEntryInfo))
                {
                    Persistence.UpdateState(newTerm, request.CandidateId);
                    return new RequestVoteResponse(CurrentTerm: newTerm, VoteGranted: true);
                }
                else
                {
                    Persistence.UpdateState(newTerm, null);
                    return new RequestVoteResponse(CurrentTerm: newTerm, VoteGranted: false);
                }
            }

            // Пока обрабатывали запрос кто-то поменял состояние, может таймер выборов сработал
            return ConsensusModule.Handle(request);
        }

        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
    }

    public override void Dispose()
    {
        _electionTimer.Timeout -= OnElectionTimerTimeout;
        _electionTimer.Stop();
        _electionTimer.Dispose();

        try
        {
            _lifetimeCts.Cancel();
            _lifetimeCts.Dispose();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    public override InstallSnapshotResponse Apply(InstallSnapshotRequest request,
        CancellationToken token = default)
    {
        if (request.Term < CurrentTerm)
        {
            return new InstallSnapshotResponse(CurrentTerm);
        }

        var state = ConsensusModule.CreateFollowerState();
        if (ConsensusModule.TryUpdateState(state, this))
        {
            _logger.Debug("Получен InstallSnapshotRequest с термом не меньше моего. Перехожу в Follower");
        }
        else
        {
            _logger.Debug("Получен InstallSnapshotRequest с термом не меньше моего, но перейти в Follower не удалось");
        }

        return ConsensusModule.Handle(request, token);
    }
}