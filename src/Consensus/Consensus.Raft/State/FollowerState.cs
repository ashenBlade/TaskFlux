using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Commands.Submit;
using Consensus.Raft.Persistence;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Raft.State;

public class FollowerState<TCommand, TResponse> : State<TCommand, TResponse>
{
    public override NodeRole Role => NodeRole.Follower;
    private readonly IStateMachineFactory<TCommand, TResponse> _stateMachineFactory;
    private readonly ICommandSerializer<TCommand> _commandCommandSerializer;
    private readonly ITimer _electionTimer;
    private readonly ILogger _logger;

    internal FollowerState(IConsensusModule<TCommand, TResponse> consensusModule,
                           IStateMachineFactory<TCommand, TResponse> stateMachineFactory,
                           ICommandSerializer<TCommand> commandCommandSerializer,
                           ITimer electionTimer,
                           ILogger logger)
        : base(consensusModule)
    {
        _stateMachineFactory = stateMachineFactory;
        _commandCommandSerializer = commandCommandSerializer;
        _electionTimer = electionTimer;
        _logger = logger;
    }

    public override void Initialize()
    {
        _electionTimer.Timeout += OnElectionTimerTimeout;
        _electionTimer.Start();
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
        if (canVote
          &&
            // У которого лог в консистентном с нашим состоянием
            !PersistenceFacade.Conflicts(request.LastLogEntryInfo))
        {
            _logger.Debug(
                "Получен RequestVote от узла за которого можем проголосовать. Id узла {NodeId}, Терм узла {Term}. Обновляю состояние",
                request.CandidateId.Id, request.CandidateTerm.Value);
            ConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, request.CandidateId);

            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }

        if (CurrentTerm < request.CandidateTerm)
        {
            _logger.Debug("Терм кандидата больше, но лог конфликтует: обновляю только терм");
            PersistenceFacade.UpdateState(request.CandidateTerm, null);
        }

        return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        _electionTimer.Stop();
        if (request.Term < CurrentTerm)
        {
            // Лидер устрел
            _electionTimer.Start();
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        if (CurrentTerm < request.Term)
        {
            // Мы отстали от общего состояния (старый терм)
            ConsensusModule.PersistenceFacade.UpdateState(request.Term, null);
        }

        if (!PersistenceFacade.PrefixMatch(request.PrevLogEntryInfo))
        {
            // Префиксы закомиченных записей лога не совпадают 
            _electionTimer.Start();
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        if (0 < request.Entries.Count)
        {
            // Если это не Heartbeat, то применить новые команды
            PersistenceFacade.InsertRange(request.Entries, request.PrevLogEntryInfo.Index + 1);
        }

        if (PersistenceFacade.CommitIndex == request.LeaderCommit)
        {
            _electionTimer.Start();
            return AppendEntriesResponse.Ok(CurrentTerm);
        }

        // В случае, если какие-то записи были закоммичены лидером, то сделать то же самое у себя.

        // Коммитим записи по индексу лидера
        PersistenceFacade.Commit(request.LeaderCommit);

        // Закоммиченные записи можно уже применять к машине состояний 
        var notApplied = PersistenceFacade.GetNotApplied();

        foreach (var entry in notApplied)
        {
            var command = _commandCommandSerializer.Deserialize(entry.Data);
            StateMachine.ApplyNoResponse(command);
        }

        // После применения команды, обновляем индекс последней примененной записи.
        // Этот индекс обновляем сразу, т.к. 
        // 1. Если возникнет исключение в работе, то это означает неправильную работу самого приложения, а не бизнес-логики
        // 2. Эта операция сразу сбрасывает данные на диск (Flush) - дорого
        PersistenceFacade.SetLastApplied(request.LeaderCommit);

        if (PersistenceFacade.IsLogFileSizeExceeded())
        {
            var snapshot = StateMachine.GetSnapshot();
            PersistenceFacade.SaveSnapshot(snapshot);
        }

        _electionTimer.Start();
        return AppendEntriesResponse.Ok(CurrentTerm);
    }

    public override SubmitResponse<TResponse> Apply(SubmitRequest<TCommand> request)
    {
        if (!request.Descriptor.IsReadonly)
        {
            return SubmitResponse<TResponse>.NotLeader;
        }

        var response = StateMachine.Apply(request.Descriptor.Command);
        return SubmitResponse<TResponse>.Success(response, false);
    }

    private void OnElectionTimerTimeout()
    {
        _logger.Debug("Сработал Election Timeout. Перехожу в состояние Candidate");
        var candidateState = ConsensusModule.CreateCandidateState();
        if (ConsensusModule.TryUpdateState(candidateState, this))
        {
            // Голосуем за себя и переходим в следующий терм
            ConsensusModule.PersistenceFacade.UpdateState(ConsensusModule.CurrentTerm.Increment(), ConsensusModule.Id);
        }
    }

    public override void Dispose()
    {
        _electionTimer.Timeout -= OnElectionTimerTimeout;
        _electionTimer.Dispose();
    }
    // TODO: тест на очищение TEMP когда из лидера в фолловера

    public override IEnumerable<InstallSnapshotResponse> Apply(InstallSnapshotRequest request,
                                                               CancellationToken token = default)
    {
        if (request.Term < CurrentTerm)
        {
            yield return new InstallSnapshotResponse(CurrentTerm);
            yield break;
        }

        if (CurrentTerm < request.Term)
        {
            PersistenceFacade.UpdateState(request.Term, null);
        }

        // 1. Обновляем файл снапшота
        foreach (var success in PersistenceFacade.InstallSnapshot(
                     new LogEntryInfo(request.LastIncludedTerm, request.LastIncludedIndex),
                     request.Snapshot, token))
        {
            yield return new InstallSnapshotResponse(CurrentTerm);
            if (!success)
            {
                yield break;
            }
        }

        _electionTimer.Start();
        yield return new InstallSnapshotResponse(CurrentTerm);
    }

    private void RestoreState()
    {
        // 1. Восстанавливаем из снапшота
        var stateMachine =
            PersistenceFacade.TryGetSnapshot(out var snapshot)
                ? _stateMachineFactory.Restore(snapshot)
                : _stateMachineFactory.CreateEmpty();

        // 2. Применяем команды из лога, если есть
        var nonApplied = PersistenceFacade.GetNotApplied();
        if (nonApplied.Count > 0)
        {
            foreach (var (_, payload) in nonApplied)
            {
                var command = _commandCommandSerializer.Deserialize(payload);
                stateMachine.ApplyNoResponse(command);
            }
        }

        // 3. Обновляем машину
        ConsensusModule.StateMachine = stateMachine;
    }
}