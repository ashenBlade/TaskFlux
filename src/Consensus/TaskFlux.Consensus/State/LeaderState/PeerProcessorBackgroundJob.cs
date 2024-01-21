using System.Diagnostics;
using Serilog;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Core;

namespace TaskFlux.Consensus.State.LeaderState;

/// <summary>
/// Фоновый обработчик соединения с другими узлами-последователями.
/// Нужен для репликации записей.
/// </summary>
public class PeerProcessorBackgroundJob<TCommand, TResponse> : IBackgroundJob, IDisposable
{
    public NodeId NodeId => _peer.Id;
    private volatile bool _disposed;
    private RaftConsensusModule<TCommand, TResponse> RaftConsensusModule => _caller.ConsensusModule;

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

    private FileSystemPersistenceFacade Persistence => RaftConsensusModule.Persistence;

    /// <summary>
    /// Узел, с которым общаемся
    /// </summary>
    private readonly IPeer _peer;

    private readonly ILogger _logger;

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
                                      LeaderState<TCommand, TResponse> caller)
    {
        var handle = new AutoResetEvent(false);
        _queue = new RequestQueue(handle);
        _queueStopEvent = handle;
        _peer = peer;
        _logger = logger;
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
        catch (Exception e)
        {
            _logger.Fatal(e, "Во время работы обработчика возникло необработанное исключение");
            throw;
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
        var peerInfo = new PeerInfo(Persistence.LastEntry.Index + 1);
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
                if (ReplicateLogReturnGreaterTerm(request.LogIndex, peerInfo, token) is { } greaterTerm)
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
                if (ReplicateLogReturnGreaterTerm(peerInfo.NextIndex, peerInfo, token) is { } greaterTerm)
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
    /// <param name="info">Вспомогательная информация про узел - на каком индексе репликации находимся</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>
    /// <see cref="Term"/> - найденный больший терм, <c>null</c> - репликация прошла успешно
    /// </returns>
    /// <exception cref="ApplicationException">
    /// Для репликации нужна запись с определенным индексом, но ее нет ни в логе, ни в снапшоте (маловероятно)
    /// </exception>
    private Term? ReplicateLogReturnGreaterTerm(int replicationIndex, PeerInfo info, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            if (!Persistence.TryGetFrom(info.NextIndex, out var entries))
            {
                _logger.Debug("В логе не оказалось записей после индекса: {Index}", info.NextIndex);
                if (Persistence.TryGetSnapshot(out var snapshot, out var lastEntry))
                {
                    _logger.Debug("Отправляю снапшот на узел");
                    var installSnapshotResponse = _peer.SendInstallSnapshot(new InstallSnapshotRequest(SavedTerm,
                        RaftConsensusModule.Id, lastEntry,
                        snapshot), token);

                    if (SavedTerm < installSnapshotResponse.CurrentTerm)
                    {
                        _logger.Information("От узла {NodeId} получен больший терм {Term}. Текущий терм: {CurrentTerm}",
                            _peer.Id, installSnapshotResponse.CurrentTerm, Persistence.CurrentTerm);
                        return installSnapshotResponse.CurrentTerm;
                    }

                    info.Set(lastEntry.Index + 1);
                    _logger.Debug("Снапшот отправлен на другой узел");
                    continue;
                }

                throw new ApplicationException(
                    $"Для репликации нужны данные лога с {info.NextIndex} индекса, но ни в логе ни в снапшоте этого лога нет");
            }

            // 1. Отправляем запрос с текущим отслеживаемым индексом узла
            AppendEntriesRequest appendEntriesRequest;
            try
            {
                appendEntriesRequest = new AppendEntriesRequest(Term: SavedTerm,
                    LeaderCommit: Persistence.CommitIndex,
                    LeaderId: RaftConsensusModule.Id,
                    PrevLogEntryInfo: Persistence.GetPrecedingEntryInfo(info.NextIndex),
                    Entries: entries);
            }
            catch (ArgumentOutOfRangeException)
            {
                /*
                 * Между первым TryGetFrom и GetPrecedingEntryInfo прошло много времени
                 * и был создан новый снапшот, поэтому info.NextIndex уже нет.
                 * На следующем круге отправим уже снапшот
                 */
                continue;
            }

            var response = _peer.SendAppendEntries(appendEntriesRequest, token);

            // 3. Если ответ успешный 
            if (response.Success)
            {
                // 3.1. Обновить nextIndex = + кол-во Entries в запросе
                info.Increment(appendEntriesRequest.Entries.Count);

                // 3.2. Если лог не до конца был синхронизирован
                if (info.NextIndex < replicationIndex)
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
            info.Decrement();

            // 5.2. Идем на следующий круг
        }

        // Единственный случай попадания сюда - токен отменен == мы больше не лидер
        // Скорее всего терм уже был обновлен
        return RaftConsensusModule.CurrentTerm;
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

    /// <summary>
    /// Информация об узле, необходимая для взаимодействия с ним в состоянии <see cref="NodeRole.Leader"/>
    /// </summary>
    private class PeerInfo
    {
        /// <summary>
        /// Индекс следующей записи в логе, которую необходимо отправить клиенту
        /// </summary>
        public int NextIndex { get; private set; }

        public PeerInfo(int nextIndex)
        {
            NextIndex = nextIndex;
        }

        /// <summary>
        /// Добавить к последнему индексу указанное число.
        /// Используется, когда запись (или несколько) были успешно реплицированы - не отправка снапшота
        /// </summary>
        /// <param name="appliedCount">Количество успешно отправленных записей</param>
        public void Increment(int appliedCount)
        {
            var nextIndex = NextIndex + appliedCount;
            Debug.Assert(0 <= nextIndex, "0 <= nextIndex",
                "Выставленный индекс следующей записи не может получиться отрицательным. Рассчитано: {0}. Кол-во примененных записей: {1}",
                nextIndex, appliedCount);
            NextIndex = nextIndex;
        }

        /// <summary>
        /// Выставить нужное число 
        /// </summary>
        /// <param name="nextIndex">Новый следующий индекс</param>
        public void Set(int nextIndex)
        {
            Debug.Assert(0 <= nextIndex, "Следующий индекс записи не может быть отрицательным",
                "Нельзя выставлять отрицательный индекс следующей записи. Попытка выставить {0}. Старый следующий индекс: {1}",
                nextIndex, NextIndex);
            NextIndex = nextIndex;
        }

        /// <summary>
        /// Откатиться назад, если узел ответил на AppendEntries <c>false</c>
        /// </summary>
        /// <exception cref="InvalidOperationException"><see cref="NextIndex"/> равен 0</exception>
        public void Decrement()
        {
            if (NextIndex is 0)
            {
                throw new InvalidOperationException("Нельзя откатиться на индекс меньше 0");
            }

            NextIndex--;
        }

        public override string ToString() => $"PeerInfo(NextIndex = {NextIndex})";
    }

    public override string ToString()
    {
        return $"PeerProcessor(NodeId = {NodeId.Id})";
    }
}