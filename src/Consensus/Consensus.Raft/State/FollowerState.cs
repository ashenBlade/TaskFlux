using Consensus.Core.Submit;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Serilog;
using TaskFlux.Models;

namespace Consensus.Raft.State;

public class FollowerState<TCommand, TResponse>
    : State<TCommand, TResponse>
{
    public override NodeRole Role => NodeRole.Follower;
    private readonly ITimer _electionTimer;
    private readonly ILogger _logger;

    internal FollowerState(IRaftConsensusModule<TCommand, TResponse> raftConsensusModule,
                           ITimer electionTimer,
                           ILogger logger)
        : base(raftConsensusModule)
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
            _logger.Verbose("Терм кандидата меньше моего терма. Отклоняю запрос");
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
        var logConflicts = PersistenceFacade.Conflicts(request.LastLogEntryInfo);
        if (canVote && !logConflicts)
        {
            _logger.Debug(
                "Получен RequestVote от узла за которого можем проголосовать. Id узла {NodeId}, Терм узла {Term}. Обновляю состояние",
                request.CandidateId.Id, request.CandidateTerm.Value);

            RaftConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, request.CandidateId);

            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }

        if (CurrentTerm < request.CandidateTerm)
        {
            if (logConflicts)
            {
                _logger.Debug(
                    "Терм кандидата больше, но лог конфликтует: обновляю только терм. Кандидат: {CandidateId}. Моя последняя запись: {MyLastEntry}. Его последняя запись: {CandidateLastEntry}",
                    request.CandidateId, PersistenceFacade.LastEntry, request.LastLogEntryInfo);
            }
            else
            {
                _logger.Debug(
                    "Терм кандидата больше, но голос в терме уже отдал: обновляю только терм. Кандидат: {CandidateId}",
                    request.CandidateId);
            }

            PersistenceFacade.UpdateState(request.CandidateTerm, null);
        }

        return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        if (request.Term < CurrentTerm)
        {
            // Лидер устрел
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        using var _ = ElectionTimerScope.BeginScope(_electionTimer);
        if (CurrentTerm < request.Term)
        {
            _logger.Information("Получен AppendEntries с большим термом {GreaterTerm}. Старый терм: {CurrentTerm}",
                request.Term, CurrentTerm);
            // Мы отстали от общего состояния (старый терм)
            RaftConsensusModule.PersistenceFacade.UpdateState(request.Term, null);
        }

        if (!PersistenceFacade.PrefixMatch(request.PrevLogEntryInfo))
        {
            // Префиксы закомиченных записей лога не совпадают 
            _logger.Information(
                "Текущий лог не совпадает с логом узла {NodeId}. Моя последняя запись: {MyLastEntry}. Его предыдущая запись: {HisLastEntry}",
                request.LeaderId, PersistenceFacade.LastEntry, request.PrevLogEntryInfo);
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        if (0 < request.Entries.Count)
        {
            PersistenceFacade.InsertBufferRange(request.Entries, request.PrevLogEntryInfo.Index + 1);
        }

        if (PersistenceFacade.CommitIndex == request.LeaderCommit)
        {
            return AppendEntriesResponse.Ok(CurrentTerm);
        }

        // Коммитим записи по индексу лидера
        PersistenceFacade.Commit(request.LeaderCommit);

        // После применения команды, обновляем индекс последней примененной записи.
        // Этот индекс обновляем сразу, т.к. 
        // 1. Если возникнет исключение в работе, то это означает неправильную работу самого приложения, а не бизнес-логики
        // 2. Эта операция сразу сбрасывает данные на диск (Flush) - дорого
        PersistenceFacade.SetLastApplied(request.LeaderCommit);

        if (PersistenceFacade.IsLogFileSizeExceeded())
        {
            _logger.Information("Размер файла лога превышен. Создаю снапшот");
            var oldSnapshot = PersistenceFacade.TryGetSnapshot(out var s)
                                  ? s
                                  : null;
            var deltas = PersistenceFacade.LogStorage.ReadAll().Select(x => x.Data);
            var newSnapshot = ApplicationFactory.CreateSnapshot(oldSnapshot, deltas);
            PersistenceFacade.SaveSnapshot(newSnapshot);
        }

        return AppendEntriesResponse.Ok(CurrentTerm);
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
        _logger.Debug("Сработал Election Timeout. Перехожу в состояние Candidate");
        var candidateState = RaftConsensusModule.CreateCandidateState();
        if (RaftConsensusModule.TryUpdateState(candidateState, this))
        {
            // Голосуем за себя и переходим в следующий терм
            RaftConsensusModule.PersistenceFacade.UpdateState(RaftConsensusModule.CurrentTerm.Increment(),
                RaftConsensusModule.Id);
        }
    }

    public override void Dispose()
    {
        _electionTimer.Timeout -= OnElectionTimerTimeout;
        _electionTimer.Dispose();
    }

    public override IEnumerable<InstallSnapshotResponse> Apply(InstallSnapshotRequest request,
                                                               CancellationToken token = default)
    {
        if (request.Term < CurrentTerm)
        {
            _logger.Information("Терм узла меньше моего. Отклоняю InstallSnapshotRequest");
            yield return new InstallSnapshotResponse(CurrentTerm);
            yield break;
        }

        using var _ = ElectionTimerScope.BeginScope(_electionTimer);
        _logger.Information("Начинаю обработку InstallSnapshot");
        _logger.Debug("Получен снапшот с индексом {Index} и термом {Term}", request.LastEntry.Index,
            request.LastEntry.Term);
        if (CurrentTerm < request.Term)
        {
            _logger.Information("Терм лидера больше моего. Обновляю терм до {Term}", request.Term);
            NodeId? votedFor = null;
            if (PersistenceFacade.VotedFor is null || PersistenceFacade.VotedFor == request.LeaderId)
            {
                votedFor = request.LeaderId;
            }

            PersistenceFacade.UpdateState(request.Term, votedFor);
        }

        _electionTimer.Stop();
        _electionTimer.Schedule();
        _logger.Debug("Начинаю получать чанки снашота");
        token.ThrowIfCancellationRequested();
        foreach (var success in PersistenceFacade.InstallSnapshot(request.LastEntry,
                     request.Snapshot, token))
        {
            _electionTimer.Stop();
            _logger.Debug("Очередной чанк снапшота установлен. Возвращаю ответ");
            yield return new InstallSnapshotResponse(CurrentTerm);

            if (!success)
            {
                yield break;
            }
        }

        PersistenceFacade.SetLastApplied(PersistenceFacade.CommitIndex);

        yield return new InstallSnapshotResponse(CurrentTerm);
    }
}