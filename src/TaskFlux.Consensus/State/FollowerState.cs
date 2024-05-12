using Serilog;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Core;

namespace TaskFlux.Consensus.State;

public class
    FollowerState<TCommand, TResponse>
    : State<TCommand, TResponse>
{
    public override NodeId? LeaderId => _leaderId;
    private NodeId? _leaderId;

    public override NodeRole Role => NodeRole.Follower;
    private readonly ITimer _electionTimer;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new();

    internal FollowerState(RaftConsensusModule<TCommand, TResponse> consensusModule,
        ITimer electionTimer,
        ILogger logger)
        : base(consensusModule)
    {
        _electionTimer = electionTimer;
        _logger = logger;
    }

    public override void Initialize()
    {
        _electionTimer.Timeout += OnElectionTimerTimeout;
        _electionTimer.Schedule();
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        // Мы в более актуальном Term'е
        if (request.CandidateTerm < CurrentTerm)
        {
            _logger.Information("От узла {NodeId} получен запрос с меньшим термом - {OldTerm}. Мой терм {MyTerm}",
                request.CandidateId, request.CandidateId, CurrentTerm);
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
        }

        // Флаг возможности отдать голос,
        // так как в каждом терме мы можем отдать голос только за 1 кандидата
        var canVote =
            // Ранее не голосовали
            VotedFor is null
            ||
            // Текущий лидер/кандидат опять посылает этот запрос (почему бы не согласиться)
            VotedFor == request.CandidateId;

        // Отдать свободный голос можем только за кандидата 
        var isUpToDate = Persistence.IsUpToDate(request.LastLogEntryInfo);
        if (canVote && isUpToDate)
        {
            _logger.Information("Отдаю голос за узел {CandidateId}", request.CandidateId);
            var newTerm = request.CandidateTerm;

            // Для оптимизации состояние обновляем, только если что-то изменилось
            var shouldUpdateState = !(newTerm == CurrentTerm && request.CandidateId == VotedFor);
            if (shouldUpdateState)
            {
                Persistence.UpdateState(newTerm, request.CandidateId);
            }

            return new RequestVoteResponse(CurrentTerm: newTerm, VoteGranted: true);
        }

        _logger.Information("Узел {NodeId} имеет больший терм, но лог конфликтует - Голос не отдаю",
            request.CandidateId);
        Term responseTerm;
        if (CurrentTerm < request.CandidateTerm)
        {
            Persistence.UpdateState(request.CandidateTerm, null);
            responseTerm = request.CandidateTerm;
        }
        else
        {
            responseTerm = CurrentTerm;
        }

        return new RequestVoteResponse(CurrentTerm: responseTerm, VoteGranted: false);
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        if (request.Term < CurrentTerm)
        {
            // Лидер устарел/отстал
            _logger.Information("Узел {NodeId} отправил AppendEntries, но имеет меньший терм", request.LeaderId);
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        using var _ = ElectionTimerScope.BeginScope(_electionTimer);
        if (CurrentTerm < request.Term)
        {
            // Мы отстали от общего состояния (старый терм)
            _logger.Information(
                "От узла {NodeId} получен AppendEntries с большим термом {GreaterTerm}. Старый терм: {CurrentTerm}",
                request.LeaderId,
                request.Term, CurrentTerm);
            Persistence.UpdateState(request.Term, null);
        }

        if (!Persistence.PrefixMatch(request.PrevLogEntryInfo))
        {
            _logger.Information("Мой лог и лог узла {NodeId} не совпадают", request.LeaderId);
            return AppendEntriesResponse.Fail(request.Term);
        }

        if (0 < request.Entries.Count)
        {
            Persistence.InsertRange(request.Entries, request.PrevLogEntryInfo.Index + 1);
        }

        if (Persistence.CommitIndex == request.LeaderCommit)
        {
            _leaderId = request.LeaderId;
            return AppendEntriesResponse.Ok(request.Term);
        }

        // Дополнительная проверка того, что не выходим за кол-во записей у себя же
        if (Persistence.CommitIndex < request.LeaderCommit)
        {
            var commitIndex = Math.Min(request.LeaderCommit, Persistence.LastEntry.Index);
            Persistence.Commit(commitIndex);
        }
        else if (request.LeaderCommit < Persistence.CommitIndex)
        {
            _logger.Warning(
                "Лидер передал индекс коммита меньше, чем у меня. Индекс коммита лидера: {LeaderCommitIndex}. Текущий индекс коммита: {CommitIndex}",
                request.LeaderCommit, Persistence.CommitIndex);
        }

        if (Persistence.ShouldCreateSnapshot())
        {
            _logger.Information("Создаю снапшот приложения");
            var oldSnapshot = Persistence.TryGetSnapshot(out var s, out var _)
                ? s
                : null;
            var deltas = Persistence.ReadCommittedDeltaFromPreviousSnapshot();
            var newSnapshot = ApplicationFactory.CreateSnapshot(oldSnapshot, deltas);
            var lastIncludedEntry = Persistence.GetEntryInfo(Persistence.CommitIndex);
            Persistence.SaveSnapshot(newSnapshot, lastIncludedEntry);
        }

        _leaderId = request.LeaderId;
        return AppendEntriesResponse.Ok(request.Term);
    }

    private readonly record struct ElectionTimerScope(ITimer Timer) : IDisposable
    {
        public void Dispose()
        {
            Timer.Schedule();
        }

        public static ElectionTimerScope BeginScope(ITimer timer)
        {
            timer.Stop();
            return new ElectionTimerScope(timer);
        }
    }

    public override SubmitResponse<TResponse> Apply(TCommand command, CancellationToken token = default)
    {
        return SubmitResponse<TResponse>.NotLeader;
    }

    private void OnElectionTimerTimeout()
    {
        var candidateState = ConsensusModule.CreateCandidateState();
        if (ConsensusModule.TryUpdateState(candidateState, this))
        {
            _logger.Debug("Сработал Election Timeout. Стал кандидатом");
            // Голосуем за себя и переходим в следующий терм
            ConsensusModule.Persistence.UpdateState(ConsensusModule.CurrentTerm.Increment(),
                ConsensusModule.Id);
        }
        else
        {
            _logger.Debug("Сработал таймер выборов, но перейти в кандидата не удалось: состояние уже изменилось");
        }
    }

    public override void Dispose()
    {
        _electionTimer.Timeout -= OnElectionTimerTimeout;
        _electionTimer.Dispose();
    }

    public override InstallSnapshotResponse Apply(InstallSnapshotRequest request,
        CancellationToken token = default)
    {
        if (request.Term < CurrentTerm)
        {
            _logger.Debug("Терм узла меньше моего. Отклоняю InstallSnapshot запрос");
            return new InstallSnapshotResponse(CurrentTerm);
        }

        using var _ = ElectionTimerScope.BeginScope(_electionTimer);
        _logger.Debug("Получен снапшот с индексом {Index} и термом {Term}", request.LastEntry.Index,
            request.LastEntry.Term);

        if (CurrentTerm < request.Term)
        {
            Persistence.UpdateState(request.Term, null);
        }

        _electionTimer.Stop();
        _electionTimer.Schedule();
        _logger.Debug("Начинаю получать чанки снапшота");
        token.ThrowIfCancellationRequested();
        _logger.Information("Начинаю устанавливать снапшот с последней командой {LastApplied}", request.LastEntry);
        var snapshotWriter = Persistence.CreateSnapshot(request.LastEntry);
        try
        {
            using var snapshotCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            using var registration = _cts.Token.UnsafeRegister(cts =>
            {
                try
                {
                    ((CancellationTokenSource)cts!).Cancel();
                }
                catch (ObjectDisposedException)
                {
                }
            }, snapshotCts);
            var t = snapshotCts.Token;
            foreach (var chunk in request.Snapshot.GetAllChunks(t))
            {
                using var scope = ElectionTimerScope.BeginScope(_electionTimer);
                snapshotWriter.InstallChunk(chunk.Span, t);
            }

            snapshotWriter.Commit();
        }
        catch (Exception)
        {
            snapshotWriter.Discard();
            throw;
        }

        _logger.Information("Снапшот установлен");

        _leaderId = request.LeaderId;
        return new InstallSnapshotResponse(request.Term);
    }
}