using Serilog;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Core;

namespace TaskFlux.Consensus.State.LeaderState;

/// <summary>
/// Фоновый обработчик соединения с другими узлами-последователями.
/// Нужен для репликации записей.
/// </summary>
internal class PeerProcessorBackgroundJob<TCommand, TResponse> : IBackgroundJob, IDisposable
{
    public NodeId NodeId => _peer.Id;
    private volatile bool _disposed;
    private RaftConsensusModule<TCommand, TResponse> ConsensusModule => _caller.ConsensusModule;

    /// <summary>
    /// Терм, в котором начали работу
    /// </summary>
    /// <remarks>
    /// Не использовать свойство <see cref="RaftConsensusModule{TCommand,TResponse}.CurrentTerm"/>,
    /// т.к. это приведет к ошибкам. Например,
    /// 1. Мы потеряли соединение с узлом X
    /// 2. Узел X восстановился
    /// 3. Узел X перешел в новый терм по таймауту
    /// 4. Узел X стал новым лидером, а мы (узел) - последователями
    /// 5. Этот поток-обработчик до сих пор не установили соединение с узлом X - НО ТЕРМ УЖЕ ПОМЕНЯЛСЯ
    /// 6. Соединение с узлом X восстановилось (для этого обработчика)
    /// 7. Узел X отвергает все наши запросы, но мы НЕ МОЖЕМ ПОНЯТЬ, ЧТО ПЕРЕШЛИ В НОВЫЙ ТЕРМ И НУЖНО ЗАКОНЧИТЬ РАБОТУ
    /// </remarks>
    private Term SavedTerm { get; }

    private IPersistence Persistence => ConsensusModule.Persistence;

    /// <summary>
    /// Узел, с которым общаемся
    /// </summary>
    private readonly IPeer _peer;

    private readonly ILogger _logger;
    private readonly PeerReplicationState _replicationState;
    private readonly ReplicationWatcher _watcher;

    /// <summary>
    /// Очередь команд для потока обработчика
    /// </summary>
    private readonly RequestQueue _queue;

    /// <summary>
    /// Объект состояния лидера, который нас создал.
    /// Работает на него
    /// </summary>
    private readonly LeaderState<TCommand, TResponse> _caller;

    private readonly AutoResetEvent _queueStopEvent;

    public PeerProcessorBackgroundJob(IPeer peer,
                                      ILogger logger,
                                      Term savedTerm,
                                      PeerReplicationState replicationState,
                                      ReplicationWatcher watcher,
                                      LeaderState<TCommand, TResponse> caller)
    {
        var handle = new AutoResetEvent(false);
        _queue = new RequestQueue(handle);
        _queueStopEvent = handle;
        _peer = peer;
        _logger = logger;
        _replicationState = replicationState;
        _watcher = watcher;
        _caller = caller;
        SavedTerm = savedTerm;
    }

    public bool Replicate(LogReplicationRequest request)
    {
        return _queue.Add(request);
    }

    public void Run(CancellationToken token)
    {
        try
        {
            ProcessPeer(token);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void ProcessPeer(CancellationToken token)
    {
        /*
         * Обновление состояния нужно делать не здесь (через этот поток),
         * т.к. это вызовет дедлок:
         * - Вызываем TryUpdateState
         * - TryUpdateState вызывает Dispose у нашего состояния лидера
         * - Состояние лидера вызывает Join для каждого потока
         * - Но т.к. вызвали мы, то получается ВЫЗЫВАЕМ Thread.Join ДЛЯ САМОГО СЕБЯ
         *
         * Если нашли такое - поставим флаг.
         * От лидера все равно запрос и там обновим состояние.
         */
        Term? foundGreaterTerm = null;
        _logger.Information("Обработчик узла начинает работу");
        foreach (var heartbeatOrRequest in _queue.ReadAllRequests(token))
        {
            // На предыдущих шагах нашли больший терм
            // Дальше узел станет последователем, а пока завершаем все запросы
            if (foundGreaterTerm is { } term)
            {
                // Heartbeat пропускаем
                if (heartbeatOrRequest.TryGetRequest(out var r))
                {
                    // Если работа закончена - оповестить всех о конце (можно не выставлять терм, т.к. приложение закрывается)
                    r.NotifyFoundGreaterTerm(term);
                }
                else if (heartbeatOrRequest.TryGetHeartbeat(out var h))
                {
                    h.NotifyFoundGreaterTerm(term);
                }

                continue;
            }

            if (heartbeatOrRequest.TryGetRequest(out var request))
            {
                _logger.Debug("Получен запрос для репликации {Index} записи", request.LogIndex);
                if (ReplicateLogReturnGreaterTerm(request.LogIndex, token) is { } greaterTerm)
                {
                    _logger.Debug("При отправке AppendEntries узел ответил большим термом");
                    request.NotifyFoundGreaterTerm(greaterTerm);
                    foundGreaterTerm = greaterTerm;
                }
                else
                {
                    _logger.Debug("Репликация {Index} индекса закончена", request.LogIndex);
                    request.NotifyComplete();
                }
            }
            else if (heartbeatOrRequest.TryGetHeartbeat(out var heartbeat))
            {
                if (ReplicateLogReturnGreaterTerm(_replicationState.NextIndex, token) is { } greaterTerm)
                {
                    _logger.Debug("При отправке Heartbeat запроса узел ответил большим термом");
                    heartbeat.NotifyFoundGreaterTerm(greaterTerm);
                    foundGreaterTerm = greaterTerm;
                }
                else
                {
                    heartbeat.NotifySuccess();
                }
            }
        }

        _logger.Information("Обработчик узла заканчивает работу");
    }

    /// <summary>
    /// Основной метод для обработки репликации лога.
    /// Возвращаемое значение - флаг того, что репликация прошла успешно,
    /// по большей части нужна только если реплицируем для Submit запроса, а не Heartbeat.
    /// </summary>
    /// <param name="replicationIndex">
    /// Индекс, до которого нужно реплицировать лог
    /// </param>
    /// <param name="token">Токен отмены</param>
    /// <returns>
    /// <see cref="Term"/> - найденный больший терм, <c>null</c> - репликация прошла успешно
    /// </returns>
    /// <exception cref="ApplicationException">
    /// Для репликации нужна запись с определенным индексом, но ее нет ни в логе, ни в снапшоте (маловероятно)
    /// </exception>
    private Term? ReplicateLogReturnGreaterTerm(Lsn replicationIndex, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            if (!Persistence.TryGetFrom(_replicationState.NextIndex, out var entries, out var prevLogEntry))
            {
                _logger.Debug("В логе не оказалось записей после индекса: {Index}", _replicationState.NextIndex);
                if (Persistence.TryGetSnapshot(out var snapshot, out var lastEntry))
                {
                    _logger.Debug("Отправляю снапшот на узел");
                    var installSnapshotResponse = _peer.SendInstallSnapshot(new InstallSnapshotRequest(SavedTerm,
                        ConsensusModule.Id, lastEntry,
                        snapshot), token);

                    if (SavedTerm < installSnapshotResponse.CurrentTerm)
                    {
                        _logger.Information("От узла {NodeId} получен больший терм {Term}. Текущий терм: {CurrentTerm}",
                            _peer.Id, installSnapshotResponse.CurrentTerm, Persistence.CurrentTerm);
                        return installSnapshotResponse.CurrentTerm;
                    }

                    if (token.IsCancellationRequested)
                    {
                        break;
                    }

                    _replicationState.Set(lastEntry.Index + 1);
                    _watcher.Notify();
                    _logger.Debug("Снапшот отправлен на другой узел");
                    continue;
                }

                throw new ApplicationException(
                    $"Для репликации нужны данные лога с {_replicationState.NextIndex} индекса, но ни в логе ни в снапшоте этого лога нет");
            }

            // 1. Отправляем запрос с текущим отслеживаемым индексом узла

            var response = _peer.SendAppendEntries(new AppendEntriesRequest(Term: SavedTerm,
                    LeaderCommit: Persistence.CommitIndex,
                    LeaderId: ConsensusModule.Id,
                    PrevLogEntryInfo: prevLogEntry,
                    Entries: entries),
                token);

            // 3. Если ответ успешный 
            if (response.Success)
            {
                if (token.IsCancellationRequested)
                {
                    break;
                }

                // 3.1. Обновить nextIndex = + кол-во Entries в запросе
                _replicationState.Increment(entries.Count);
                _watcher.Notify();

                // 3.2. Если лог не до конца был синхронизирован
                if (_replicationState.NextIndex < replicationIndex)
                {
                    // Заходим на новый круг и отправляем еще
                    continue;
                }

                // 3.3. Уведомляем об успешной отправке команды на узел
                return null;
            }

            // Дальше узел отказался принимать наш запрос (Success = false)

            // 4. Если вернувшийся терм больше нашего
            if (SavedTerm < response.Term)
            {
                // Уведомляем о большем терме. 
                // Обновление состояния произойдет позже
                return response.Term;
            }

            // 5.1. Декрементируем последние записи лога
            _replicationState.Decrement();

            // Не нужно уведомлять, т.к. записи у нас уже были закоммичены
            // _watcher.Notify();

            // 5.2. Идем на следующий круг
        }

        // Единственный случай попадания сюда - токен отменен == мы больше не лидер
        // Скорее всего терм уже был обновлен
        return ConsensusModule.CurrentTerm;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _queueStopEvent.Set();
        _queue.Dispose();
        _queueStopEvent.Dispose();
    }

    public bool TryNotifyHeartbeat(out HeartbeatSynchronizer o)
    {
        return _queue.TryAddHeartbeat(out o);
    }


    public override string ToString()
    {
        return $"PeerProcessor(NodeId = {NodeId.Id})";
    }
}