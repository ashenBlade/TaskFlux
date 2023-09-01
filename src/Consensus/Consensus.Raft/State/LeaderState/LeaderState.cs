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

    private (ThreadPeerProcessor<TCommand, TResponse> Processor, ITimer Timer)[] _peerProcessors =
        Array.Empty<(ThreadPeerProcessor<TCommand, TResponse>, ITimer)>();

    private readonly ICommandSerializer<TCommand> _commandSerializer;
    private readonly ITimerFactory _timerFactory;

    /// <summary>
    /// Токен отмены отменяющийся при смене роли с лидера на последователя (другого быть не может)
    /// </summary>
    private readonly CancellationTokenSource _becomeFollowerTokenSource = new();

    internal LeaderState(IConsensusModule<TCommand, TResponse> consensusModule,
                         ILogger logger,
                         ICommandSerializer<TCommand> commandSerializer,
                         ITimerFactory timerFactory)
        : base(consensusModule)
    {
        _logger = logger;
        _commandSerializer = commandSerializer;
        _timerFactory = timerFactory;
    }

    public override void Initialize()
    {
        _logger.Verbose("Инициализируются потоки обработчиков узлов");
        _peerProcessors = CreatePeerProcessors();
        _logger.Verbose("Потоки обработчиков запускаются");

        Array.ForEach(_peerProcessors, p =>
        {
            // Вместе с таймером уйдет и остальное
            p.Timer.Timeout += () =>
            {
                // Конкретно здесь нужно обрабатывать обновление состояния
                if (p.Processor.TryNotifyHeartbeat(out var synchronizer))
                {
                    if (synchronizer.TryWaitGreaterTerm(out var greaterTerm))
                    {
                        var follower = ConsensusModule.CreateFollowerState();
                        if (ConsensusModule.TryUpdateState(follower, this))
                        {
                            ConsensusModule.PersistenceFacade.UpdateState(greaterTerm, null);
                        }

                        return;
                    }
                }

                // Запускаем таймер заново
                p.Timer.Start();
            };
            p.Processor.Start(_becomeFollowerTokenSource.Token);
            p.Timer.Start();
        });
        _logger.Verbose("Потоки обработчиков узлов запущены");
    }

    private (ThreadPeerProcessor<TCommand, TResponse>, ITimer)[] CreatePeerProcessors()
    {
        var peers = PeerGroup.Peers;
        var processors = new (ThreadPeerProcessor<TCommand, TResponse>, ITimer)[peers.Count];
        for (var i = 0; i < processors.Length; i++)
        {
            var peer = peers[i];
            var processor = new ThreadPeerProcessor<TCommand, TResponse>(peer,
                _logger.ForContext("SourceContext", $"PeerProcessor({peer.Id.Id})"), this);
            var timer = _timerFactory.CreateTimer();
            processors[i] = ( processor, timer );
        }

        return processors;
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        if (request.Term <= CurrentTerm)
        {
            if (request.Term == CurrentTerm)
            {
                _logger.Warning("От узла {NodeId} пришел запрос. Наши термы совпадают: {Term}", request.LeaderId.Id,
                    request.Term.Value);
            }
            else
            {
                _logger.Debug("От узла {NodeId} пришел AppendEntries запрос. Его меньше терм меньше моего: {Term}",
                    request.LeaderId.Id, request.Term.Value);
            }

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

        var logConflicts = PersistenceFacade.Conflicts(request.LastLogEntryInfo);

        if (CurrentTerm < request.CandidateTerm)
        {
            var followerState = ConsensusModule.CreateFollowerState();

            if (ConsensusModule.TryUpdateState(followerState, this))
            {
                ConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, null);
                return new RequestVoteResponse(CurrentTerm, !logConflicts);
            }

            // Уже есть новое состояние - пусть оно ответит
            return ConsensusModule.Handle(request);
        }

        var canVote =
            // Ранее не голосовали
            VotedFor is null
          ||
            // В этом терме мы за него уже проголосовали
            VotedFor == request.CandidateId;

        // Отдать свободный голос можем только за кандидата 
        if (canVote
&&                         // За которого можем проголосовать и
            !logConflicts) // У которого лог не хуже нашего
        {
            var followerState = ConsensusModule.CreateFollowerState();
            if (ConsensusModule.TryUpdateState(followerState, this))
            {
                ConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, request.CandidateId);
                return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
            }

            return ConsensusModule.Handle(request);
        }

        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
    }

    public override void Dispose()
    {
        Array.ForEach(_peerProcessors, static p =>
        {
            p.Timer.Dispose();
            p.Processor.Dispose();
        });

        // Сначала отменяем токен - после этого очередь должна разгрестись
        try
        {
            _becomeFollowerTokenSource.Cancel();
            // _becomeFollowerTokenSource.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // Скорее всего это из-за потока обработчика узла,
            // который получил больший терм и решил нас обновить
        }
    }

    public override InstallSnapshotResponse Apply(InstallSnapshotRequest request, CancellationToken token = default)
    {
        if (request.Term < CurrentTerm)
        {
            return new InstallSnapshotResponse(CurrentTerm);
        }

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
        var success = TryReplicate(appended.Index, out var greaterTerm);
        if (!success)
        {
            if (ConsensusModule.TryUpdateState(ConsensusModule.CreateFollowerState(), this))
            {
                PersistenceFacade.UpdateState(greaterTerm, null);
                return SubmitResponse<TResponse>.NotLeader;
            }

            return ConsensusModule.Handle(request);
        }

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

    private bool TryReplicate(int appendedIndex, out Term greaterTerm)
    {
        if (_peerProcessors.Length == 0)
        {
            // Кроме нас в кластере никого нет
            greaterTerm = Term.Start;
            return true;
        }

        using var request = new LogReplicationRequest(PeerGroup, appendedIndex);
        Array.ForEach(_peerProcessors, p => p.Processor.Replicate(request));
        request.Wait(_becomeFollowerTokenSource.Token);
        return !request.TryGetGreaterTerm(out greaterTerm);
    }

    /// <summary>
    /// Флаг сигнализирующий о том, что я (объект, лидер этого терма) все еще активен,
    /// т.е. состояние не изменилось
    /// </summary>
    private bool IsStillLeader => !_becomeFollowerTokenSource.IsCancellationRequested;
}