using System.Diagnostics;
using Serilog;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Core;

namespace TaskFlux.Consensus.State.LeaderState;

public class LeaderState<TCommand, TResponse> : State<TCommand, TResponse>
{
    public override NodeRole Role => NodeRole.Leader;

    // Логично, что ID лидера - наш ID
    public override NodeId? LeaderId => Id;
    private readonly ILogger _logger;

    private (PeerProcessorBackgroundJob<TCommand, TResponse> Processor, ITimer Timer)[] _peerProcessors =
        Array.Empty<(PeerProcessorBackgroundJob<TCommand, TResponse>, ITimer)>();

    private readonly IDeltaExtractor<TResponse> _deltaExtractor;
    private readonly ITimerFactory _timerFactory;
    private IApplication<TCommand, TResponse>? _application;

    /// <summary>
    /// Токен времени жизни текущего лидера.
    /// Отменяется при переходе в новое состояние
    /// </summary>
    private readonly CancellationTokenSource _lifetimeCts = new();

    internal LeaderState(RaftConsensusModule<TCommand, TResponse> consensusModule,
                         ILogger logger,
                         IDeltaExtractor<TResponse> deltaExtractor,
                         ITimerFactory timerFactory)
        : base(consensusModule)
    {
        _logger = logger;
        _deltaExtractor = deltaExtractor;
        _timerFactory = timerFactory;
    }

    public override void Initialize()
    {
        _peerProcessors = CreatePeerProcessors();
        var oldSnapshot = Persistence.TryGetSnapshot(out var s, out _)
                              ? s
                              : null;

        _logger.Information("Восстанавливаю предыдущее состояние");
        var deltas = Persistence.ReadCommittedDelta();
        _application = ApplicationFactory.Restore(oldSnapshot, deltas);
        Array.ForEach(_peerProcessors, p =>
        {
            // Вместе с таймером уйдет и остальное
            p.Timer.Timeout += () =>
            {
                // Конкретно здесь нужно обрабатывать обновление состояния
                if (p.Processor.TryNotifyHeartbeat(out var synchronizer))
                {
                    try
                    {
                        if (synchronizer.TryWaitGreaterTerm(out var greaterTerm))
                        {
                            var follower = ConsensusModule.CreateFollowerState();
                            if (ConsensusModule.TryUpdateState(follower, this))
                            {
                                ConsensusModule.Persistence.UpdateState(greaterTerm, null);
                            }

                            return;
                        }
                    }
                    finally
                    {
                        synchronizer.Dispose();
                    }
                }

                // Запускаем таймер заново
                p.Timer.Schedule();
            };

            BackgroundJobQueue.Accept(p.Processor, _lifetimeCts.Token);
            p.Timer.ForceRun();
            _logger.Debug("Обработчик для узла {NodeId} запущен", p.Processor.NodeId);
        });
        _logger.Verbose("Потоки обработчиков узлов запущены");
    }

    private (PeerProcessorBackgroundJob<TCommand, TResponse>, ITimer)[] CreatePeerProcessors()
    {
        var peers = PeerGroup.Peers;
        var processors = new (PeerProcessorBackgroundJob<TCommand, TResponse>, ITimer)[peers.Count];
        var replicationStates = new PeerReplicationState[peers.Count];
        var lastEntryIndex = Persistence.LastEntry.Index + 1;
        for (var i = 0; i < replicationStates.Length; i++)
        {
            replicationStates[i] = new PeerReplicationState(lastEntryIndex);
        }

        var replicationWatcher = new ReplicationWatcher(replicationStates, Persistence);
        for (var i = 0; i < processors.Length; i++)
        {
            var peer = peers[i];
            var info = replicationStates[i];
            var processor = new PeerProcessorBackgroundJob<TCommand, TResponse>(peer,
                _logger.ForContext("SourceContext", $"PeerProcessor({peer.Id.Id})"), CurrentTerm, info,
                replicationWatcher, this);
            var timer = _timerFactory.CreateHeartbeatTimer();
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

        var isUpToDate = Persistence.IsUpToDate(request.LastLogEntryInfo);

        if (CurrentTerm < request.CandidateTerm)
        {
            var followerState = ConsensusModule.CreateFollowerState();

            if (ConsensusModule.TryUpdateState(followerState, this))
            {
                ConsensusModule.Persistence.UpdateState(request.CandidateTerm, null);
                return new RequestVoteResponse(CurrentTerm, isUpToDate);
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
&&                      // За которого можем проголосовать и
            isUpToDate) // У которого лог не хуже нашего
        {
            var followerState = ConsensusModule.CreateFollowerState();
            if (ConsensusModule.TryUpdateState(followerState, this))
            {
                ConsensusModule.Persistence.UpdateState(request.CandidateTerm, request.CandidateId);
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
        try
        {
            _lifetimeCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        Array.ForEach(_peerProcessors, static p =>
        {
            p.Timer.Dispose();
            p.Processor.Dispose();
        });

        // Сначала отменяем токен - после этого очередь должна разгрестись
        try
        {
            _lifetimeCts.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // Скорее всего это из-за потока обработчика узла,
            // который получил больший терм и решил нас обновить
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
        ConsensusModule.TryUpdateState(state, this);
        return ConsensusModule.Handle(request, token);
    }

    public override SubmitResponse<TResponse> Apply(TCommand command, CancellationToken token = default)
    {
        Debug.Assert(_application is not null, "_application is not null",
            "Приложение не было инициализировано на момент обработки запроса");
        _logger.Debug("Получил новую команду: {Command}", command);

        var response = _application.Apply(command);
        if (!_deltaExtractor.TryGetDelta(response, out var delta))
        {
            // Если команда не выполнила модификаций (дельты нет),
            // то сразу возвращаем результат - без необходимости репликации/фиксации
            return SubmitResponse<TResponse>.Success(response, true);
        }

        // Добавляем команду в буфер
        _logger.Verbose("Добавляю команду в лог");
        var newEntry = new LogEntry(CurrentTerm, delta);
        var appended = Persistence.Append(newEntry);

        // Сигнализируем узлам, чтобы принялись реплицировать
        _logger.Verbose("Начинаю репликацию записанной команды");
        var success = TryReplicate(appended, out var greaterTerm);
        if (!success)
        {
            _logger.Verbose("Команду реплицировать не удалось: состояние поменялось");
            if (Role != NodeRole.Leader)
            {
                // Пока выполняли запрос перестали быть лидером
                return ConsensusModule.Handle(command, token);
            }

            if (ConsensusModule.TryUpdateState(ConsensusModule.CreateFollowerState(), this))
            {
                _logger.Verbose("Стал Follower");
                Persistence.UpdateState(greaterTerm, null);
                return SubmitResponse<TResponse>.NotLeader;
            }

            return ConsensusModule.Handle(command, token);
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

        if (Persistence.ShouldCreateSnapshot())
        {
            _logger.Debug("Создаю снапшот");
            var snapshot = _application.GetSnapshot();
            Persistence.SaveSnapshot(snapshot, new LogEntryInfo(newEntry.Term, appended), token);
        }

        // Возвращаем результат
        return SubmitResponse<TResponse>.Success(response, true);
    }

    private bool TryReplicate(Lsn appendedIndex, out Term greaterTerm)
    {
        if (_peerProcessors.Length == 0)
        {
            Persistence.Commit(appendedIndex);
            greaterTerm = Term.Start;
            return true;
        }

        using var request = new LogReplicationRequest(PeerGroup, appendedIndex);
        Array.ForEach(_peerProcessors, p => p.Processor.Replicate(request));

        CancellationToken token;
        try
        {
            token = _lifetimeCts.Token;
        }
        catch (ObjectDisposedException)
        {
            greaterTerm = Term.Start;
            return false;
        }

        request.Wait(token);
        return !request.TryGetGreaterTerm(out greaterTerm);
    }

    /// <summary>
    /// Флаг сигнализирующий о том, что я (объект, лидер этого терма) все еще активен,
    /// т.е. состояние не изменилось
    /// </summary>
    private bool IsStillLeader => !_lifetimeCts.IsCancellationRequested;
}