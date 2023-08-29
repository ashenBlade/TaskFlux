using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Commands.Submit;
using Consensus.Raft.Persistence;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Raft.State.LeaderState;

public class LeaderState<TCommand, TResponse> : State<TCommand, TResponse>
{
    public override NodeRole Role => NodeRole.Leader;
    private readonly ILogger _logger;
    private readonly ThreadPeerProcessor<TCommand, TResponse>[] _peerProcessors;
    private readonly ICommandSerializer<TCommand> _commandSerializer;

    /// <summary>
    /// Токен отмены отменяющийся при смене роли с лидера на последователя (другого быть не может)
    /// </summary>
    private readonly CancellationTokenSource _becomeFollowerTokenSource = new();

    internal LeaderState(IConsensusModule<TCommand, TResponse> consensusModule,
                         ILogger logger,
                         ThreadPeerProcessor<TCommand, TResponse>[] peerProcessors,
                         ICommandSerializer<TCommand> commandSerializer)
        : base(consensusModule)
    {
        _logger = logger;
        _peerProcessors = peerProcessors;
        _commandSerializer = commandSerializer;
    }

    public override void Initialize()
    {
        Array.ForEach(_peerProcessors, p => p.Start(this, _becomeFollowerTokenSource.Token));
        HeartbeatTimer.Timeout += OnHeartbeatTimer;
        HeartbeatTimer.Start();
    }

    private void OnHeartbeatTimer()
    {
        Array.ForEach(_peerProcessors, p => p.NotifyHeartbeat());
        HeartbeatTimer.Start();
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        if (request.Term <= CurrentTerm)
        {
            // Согласно алгоритму, в каждом терме свой лидер, поэтому ситуации равенства быть не должно
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        // Прийти может только от другого лидера с большим термом

        var follower = ConsensusModule.CreateFollowerState();
        ConsensusModule.TryUpdateState(follower, this);
        return ConsensusModule.Handle(request);
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        if (request.CandidateTerm < CurrentTerm)
        {
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
        }

        if (CurrentTerm < request.CandidateTerm)
        {
            var followerState = ConsensusModule.CreateFollowerState();
            if (ConsensusModule.TryUpdateState(followerState, this))
            {
                ConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, request.CandidateId);
                ElectionTimer.Start();
            }

            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }

        var canVote =
            // Ранее не голосовали
            VotedFor is null
          ||
            // В этом терме мы за него уже проголосовали
            VotedFor == request.CandidateId;

        // Отдать свободный голос можем только за кандидата 
        if (canVote
&&                                                                  // За которого можем проголосовать и
            !PersistenceFacade.Conflicts(request.LastLogEntryInfo)) // У которого лог не хуже нашего
        {
            var followerState = ConsensusModule.CreateFollowerState();
            if (ConsensusModule.TryUpdateState(followerState, this))
            {
                ConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, request.CandidateId);
                ElectionTimer.Start();
            }

            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }

        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
    }

    public override void Dispose()
    {
        // CommandQueue.Enqueue(new StopHeartbeatTimerCommand(this, Node));
        HeartbeatTimer.Stop();
        HeartbeatTimer.Timeout -= OnHeartbeatTimer;
    }


    public override InstallSnapshotResponse Apply(InstallSnapshotRequest request, CancellationToken token = default)
    {
        if (request.Term < CurrentTerm)
        {
            return new InstallSnapshotResponse(CurrentTerm);
        }
        // TODO: тесты

        var followerState = ConsensusModule.CreateFollowerState();
        if (ConsensusModule.TryUpdateState(followerState, this))
        {
            return ConsensusModule.Handle(request, token);
        }

        return new InstallSnapshotResponse(CurrentTerm);
    }

    public override SubmitResponse<TResponse> Apply(SubmitRequest<TCommand> request)
    {
        if (request.Descriptor.IsReadonly)
        {
            // Короткий путь для readonly команд
            return SubmitResponse<TResponse>.Success(StateMachine.Apply(request.Descriptor.Command), true);
        }

        // Добавляем команду в буфер
        var newEntry = new LogEntry(CurrentTerm, _commandSerializer.Serialize(request.Descriptor.Command));
        var appended = PersistenceFacade.AppendBuffer(newEntry);

        // Сигнализируем узлам, чтобы принялись реплицировать
        Replicate(appended.Index);

        /*
         * Если мы вернулись, то это может значить 2 вещи:
         *  1. Запись успешно реплицирована
         *  2. Какой-то узел вернул больший терм и мы перешли в фолловера
         *  3. Во время отправки запросов нам пришел запрос с большим термом
         */
        if (!IsStillLeader)
        {
            return SubmitResponse<TResponse>.NotLeader;
        }

        // Применяем команду к машине состояний.
        // Лучше сначала применить и, если что не так, упасть,
        // чем закоммитить, а потом каждый раз валиться при восстановлении
        var response = StateMachine.Apply(request.Descriptor.Command);

        // Коммитим запись и применяем 
        PersistenceFacade.Commit(appended.Index);
        PersistenceFacade.SetLastApplied(appended.Index);

        /*
         * На этом моменте Heartbeat не отправляю,
         * т.к. он отправится по таймеру (он должен вызываться часто).
         * Либо придет новый запрос и он отправится вместе с AppendEntries
         */

        if (PersistenceFacade.IsLogFileSizeExceeded())
        {
            // Асинхронно это наверно делать не стоит (пока)
            var snapshot = StateMachine.GetSnapshot();
            var snapshotLastEntryInfo = PersistenceFacade.LastApplied;
            PersistenceFacade.SaveSnapshot(snapshotLastEntryInfo, snapshot, CancellationToken.None);
            PersistenceFacade.ClearCommandLog();
        }

        // Возвращаем результат
        return SubmitResponse<TResponse>.Success(response, true);
    }

    private void Replicate(int appendedIndex)
    {
        using var request = new LogReplicationRequest(PeerGroup, appendedIndex);
        Array.ForEach(_peerProcessors, p => p.Replicate(request));
        request.Wait(_becomeFollowerTokenSource.Token);
    }

    /// <summary>
    /// Флаг сигнализирующий о том, что я (объект, лидер этого терма) все еще активен,
    /// т.е. состояние не изменилось
    /// </summary>
    private bool IsStillLeader =>
        !_becomeFollowerTokenSource.IsCancellationRequested;
}