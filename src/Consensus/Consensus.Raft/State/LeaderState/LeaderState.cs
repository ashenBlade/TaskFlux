using System.Diagnostics;
using Consensus.Core.Submit;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Persistence;
using Serilog;
using TaskFlux.Models;

namespace Consensus.Raft.State.LeaderState;

public class LeaderState<TCommand, TResponse>
    : State<TCommand, TResponse>
{
    public override NodeRole Role => NodeRole.Leader;
    private readonly ILogger _logger;

    private (ThreadPeerProcessor<TCommand, TResponse> Processor, ITimer Timer)[] _peerProcessors =
        Array.Empty<(ThreadPeerProcessor<TCommand, TResponse>, ITimer)>();

    private readonly ICommandSerializer<TCommand> _commandSerializer;
    private readonly ITimerFactory _timerFactory;
    private IApplication<TCommand, TResponse>? _application;

    /// <summary>
    /// Токен отмены отменяющийся при смене роли с лидера на последователя (другого быть не может)
    /// </summary>
    private readonly CancellationTokenSource _becomeFollowerTokenSource = new();

    internal LeaderState(IRaftConsensusModule<TCommand, TResponse> raftConsensusModule,
                         ILogger logger,
                         ICommandSerializer<TCommand> commandSerializer,
                         ITimerFactory timerFactory)
        : base(raftConsensusModule)
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

        var oldSnapshot = PersistenceFacade.TryGetSnapshot(out var s)
                              ? s
                              : null;
        var deltas = PersistenceFacade.LogStorage.ReadAll().Select(x => x.Data);
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
                            var follower = RaftConsensusModule.CreateFollowerState();
                            if (RaftConsensusModule.TryUpdateState(follower, this))
                            {
                                RaftConsensusModule.PersistenceFacade.UpdateState(greaterTerm, null);
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
            p.Processor.Start(_becomeFollowerTokenSource.Token);
            // Сразу же запускаем обработчик, чтобы уведомить других об окончании выборов
            p.Timer.ForceRun();
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

        var follower = RaftConsensusModule.CreateFollowerState();
        RaftConsensusModule.TryUpdateState(follower, this);
        return RaftConsensusModule.Handle(request);
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
            var followerState = RaftConsensusModule.CreateFollowerState();

            if (RaftConsensusModule.TryUpdateState(followerState, this))
            {
                RaftConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, null);
                return new RequestVoteResponse(CurrentTerm, !logConflicts);
            }

            // Уже есть новое состояние - пусть оно ответит
            return RaftConsensusModule.Handle(request);
        }

        var canVote =
            // Ранее не голосовали
            VotedFor is null
          ||
            // В этом терме мы за него уже проголосовали
            VotedFor == request.CandidateId;

        // Отдать свободный голос можем только за кандидата 
        if (canVote
         &&                // За которого можем проголосовать и
            !logConflicts) // У которого лог не хуже нашего
        {
            var followerState = RaftConsensusModule.CreateFollowerState();
            if (RaftConsensusModule.TryUpdateState(followerState, this))
            {
                RaftConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, request.CandidateId);
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
        Array.ForEach(_peerProcessors, static p =>
        {
            p.Timer.Dispose();
            p.Processor.Dispose();
        });

        // Сначала отменяем токен - после этого очередь должна разгрестись
        try
        {
            _becomeFollowerTokenSource.Cancel();
            _becomeFollowerTokenSource.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // Скорее всего это из-за потока обработчика узла,
            // который получил больший терм и решил нас обновить
        }
    }

    public override IEnumerable<InstallSnapshotResponse> Apply(InstallSnapshotRequest request,
                                                               CancellationToken token = default)
    {
        if (request.Term < CurrentTerm)
        {
            return new[] {new InstallSnapshotResponse(CurrentTerm)};
        }

        var state = RaftConsensusModule.CreateFollowerState();
        RaftConsensusModule.TryUpdateState(state, this);
        return RaftConsensusModule.Handle(request, token);
    }

    public override SubmitResponse<TResponse> Apply(TCommand command, CancellationToken token = default)
    {
        Debug.Assert(_application is not null, "_application is not null",
            "Приложение не было инициализировано на момент обработки запроса");
        // Если команда не изменяет состояние приложения, 
        // то применяем сразу и возвращаем результат
        if (!_commandSerializer.TryGetDelta(command, out var delta))
        {
            return SubmitResponse<TResponse>.Success(_application.Apply(command), true);
        }

        // Добавляем команду в буфер
        _logger.Verbose("Записываю дельту в буфер");
        var newEntry = new LogEntry(CurrentTerm, delta);
        var appended = PersistenceFacade.AppendBuffer(newEntry);

        // Сигнализируем узлам, чтобы принялись реплицировать
        _logger.Verbose("Реплицирую команду");
        var success = TryReplicate(appended.Index, out var greaterTerm);
        if (!success)
        {
            _logger.Verbose("Команду реплицировать не удалось: состояние поменялось");
            if (Role != NodeRole.Leader)
            {
                // Пока выполняли запрос перестали быть лидером
                return RaftConsensusModule.Handle(command, token);
            }

            if (RaftConsensusModule.TryUpdateState(RaftConsensusModule.CreateFollowerState(), this))
            {
                _logger.Verbose("Стал Follower");
                PersistenceFacade.UpdateState(greaterTerm, null);
                return SubmitResponse<TResponse>.NotLeader;
            }

            return RaftConsensusModule.Handle(command, token);
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

        // Применяем команду к приложению.
        // Лучше сначала применить и, если что не так, упасть,
        // чем закоммитить, а потом каждый раз валиться при восстановлении
        var response = _application.Apply(command);

        // Коммитим запись и применяем 
        _logger.Verbose("Коммичу команду с индексом {Index}", appended.Index);
        PersistenceFacade.Commit(appended.Index);
        PersistenceFacade.SetLastApplied(appended.Index);

        /*
         * На этом моменте Heartbeat не отправляю,
         * т.к. он отправится по таймеру (он должен вызываться часто).
         * Либо придет новый запрос и он отправится вместе с AppendEntries
         */
        if (PersistenceFacade.IsLogFileSizeExceeded())
        {
            _logger.Debug("Размер файла лога превышен. Создаю снапшот");
            // Асинхронно это наверно делать не стоит (пока)
            var snapshot = _application.GetSnapshot();
            PersistenceFacade.SaveSnapshot(snapshot, token);
        }
        else
        {
            _logger.Verbose("Размер лога не превышен");
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

        CancellationToken token;
        try
        {
            token = _becomeFollowerTokenSource.Token;
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
    private bool IsStillLeader => !_becomeFollowerTokenSource.IsCancellationRequested;
}