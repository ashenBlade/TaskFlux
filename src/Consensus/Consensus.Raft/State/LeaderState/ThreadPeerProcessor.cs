using System.Diagnostics;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Persistence;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Raft.State.LeaderState;

/// <summary>
/// Реализация коммуникации с другими узлами, использующая потоки.
/// В первой версии использовался пул потоков и все было на async/await.
/// Потом отказался для большей управляемости и возможности аварийно завершиться при ошибках.
/// </summary>
internal class ThreadPeerProcessor<TCommand, TResponse> : IDisposable
{
    private volatile bool _disposed;
    private IConsensusModule<TCommand, TResponse> ConsensusModule => _caller.ConsensusModule;
    private Term CurrentTerm => ConsensusModule.CurrentTerm;
    private StoragePersistenceFacade PersistenceFacade => ConsensusModule.PersistenceFacade;

    /// <summary>
    /// Узел, с которым общаемся
    /// </summary>
    private readonly IPeer _peer;

    private readonly ILogger _logger;

    /// <summary>
    /// Фоновый поток обработчик запросов
    /// </summary>
    private readonly Thread _thread;

    /// <summary>
    /// Очередь команд для потока обработчика
    /// </summary>
    private readonly RequestQueue _queue;

    /// <summary>
    /// Объект состояния лидера, который нас создал.
    /// Работает на него
    /// </summary>
    private readonly LeaderState<TCommand, TResponse> _caller;

    /// <summary>
    /// Токен отмены, передающийся в момент вызова <see cref="Start"/>
    /// </summary>
    private CancellationToken _token;

    private readonly AutoResetEvent _queueStopEvent;

    public ThreadPeerProcessor(IPeer peer, ILogger logger, LeaderState<TCommand, TResponse> caller)
    {
        _thread = new Thread(ThreadWorker);
        var handle = new AutoResetEvent(false);
        _queue = new RequestQueue(handle);
        _queueStopEvent = handle;
        _peer = peer;
        _logger = logger;
        _caller = caller;
    }

    public void Start(CancellationToken token)
    {
        _token = token;
        _thread.Start();
    }

    public bool Replicate(LogReplicationRequest request)
    {
        return _queue.Add(request);
    }

    private void ThreadWorker()
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
        try
        {
            var peerInfo = new PeerInfo(PersistenceFacade.LastEntry.Index + 1);
            Term? foundGreaterTerm = null;
            _logger.Information("Обработчик узла начинает работу");
            foreach (var heartbeatOrRequest in _queue.ReadAllRequests(_token))
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
                    if (TryReplicateLog(request.LogIndex, peerInfo) is { } greaterTerm)
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
                    if (TryReplicateLog(peerInfo.NextIndex, peerInfo) is { } greaterTerm)
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
        catch (Exception e)
        {
            _logger.Fatal(e, "Во время работы обработчика возникло необработанное исключение");
            throw;
        }
    }

    /// <summary>
    /// Основной метод для обработки репликации лога.
    /// Возвращаемое значение - флаг того, что репликация прошла успешно,
    /// по большей части нужна только если реплицируем для Submit запроса, а не Heartbeat.
    /// </summary>
    /// <param name="replicationIndex">
    /// Индекс, до которого нужно среплицировать лог
    /// </param>
    /// <param name="info">Вспомогательная информация про узел - на каком индексе репликации находимся</param>
    /// <returns>
    /// <see cref="Term"/> - найденный больший терм, <c>null</c> - репликация прошла успешно
    /// </returns>
    /// <exception cref="ApplicationException">
    /// Для репликации нужна запись с определенным индексом, но ее нет ни в логе, ни в снапшоте (маловероятно)
    /// </exception>
    private Term? TryReplicateLog(int replicationIndex, PeerInfo info)
    {
        while (!_token.IsCancellationRequested)
        {
            if (!PersistenceFacade.TryGetFrom(info.NextIndex, out var entries))
            {
                _logger.Debug("В логе не оказалось записей после индекса: {Index}", info.NextIndex);
                if (PersistenceFacade.TryGetSnapshot(out var snapshot))
                {
                    _logger.Debug("Начинаю отправку файла снапшота на узел");
                    var lastEntry = PersistenceFacade.SnapshotStorage.LastLogEntry;
                    var installSnapshotResponses = _peer.SendInstallSnapshot(new InstallSnapshotRequest(CurrentTerm,
                        ConsensusModule.Id, lastEntry,
                        snapshot), _token);

                    var connectionBroken = false;
                    foreach (var installSnapshotResponse in installSnapshotResponses)
                    {
                        if (installSnapshotResponse is null)
                        {
                            _logger.Debug(
                                "Во время отправки файла снапшота соединение было разорвано. Прекращаю оправку");
                            connectionBroken = true;
                            break;
                        }

                        if (CurrentTerm < installSnapshotResponse.CurrentTerm)
                        {
                            // В текущей архитектуре - ничего не делаем,
                            _logger.Information(
                                "От узла {NodeId} получен больший терм {Term}. Текущий терм: {CurrentTerm}", _peer.Id,
                                installSnapshotResponse.CurrentTerm, PersistenceFacade.CurrentTerm);
                            return installSnapshotResponse.CurrentTerm;
                        }

                        // Продолжаем отправлять запросы
                    }

                    if (connectionBroken)
                    {
                        _logger.Debug(
                            "Во время отправки снапшота соединение было разорвано. Делаю повторную попытку отправки снапшота");
                        continue;
                    }

                    // Новый индекс - следующий после снапшота
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
                appendEntriesRequest = new AppendEntriesRequest(Term: CurrentTerm,
                    LeaderCommit: PersistenceFacade.CommitIndex,
                    LeaderId: ConsensusModule.Id,
                    PrevLogEntryInfo: PersistenceFacade.GetPrecedingEntryInfo(info.NextIndex),
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

            AppendEntriesResponse response;
            while (true)
            {
                var currentResponse = _peer.SendAppendEntries(appendEntriesRequest);
                _token.ThrowIfCancellationRequested();
                // 2. Если ответ не вернулся (null) - соединение было разорвано - делаем повторную попытку с переподключением
                if (currentResponse is null)
                {
                    // При повторной попытке отправки должен переподключиться
                    continue;
                }

                response = currentResponse;
                break;
            }

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
            if (CurrentTerm < response.Term)
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

        if (_thread is {IsAlive: true})
        {
            Debug.Assert(_thread.ManagedThreadId != Environment.CurrentManagedThreadId,
                "ДОЛБАЕБ!!!! Пытаешься вызвать Join для самого себя!!!");
            _thread.Join();
        }
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
                throw new InvalidOperationException("Нельзя откаться на индекс меньше 0");
            }

            NextIndex--;
        }

        public override string ToString() => $"PeerInfo(NextIndex = {NextIndex})";
    }
}