using System.Diagnostics;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Persistence;

namespace Consensus.Raft.State.LeaderState;

public class ThreadPeerProcessor<TCommand, TResponse> : IDisposable
{
    private volatile bool _disposed;
    private IConsensusModule<TCommand, TResponse> ConsensusModule => _caller.ConsensusModule;
    private Term CurrentTerm => ConsensusModule.CurrentTerm;
    private StoragePersistenceFacade PersistenceFacade => ConsensusModule.PersistenceFacade;

    /// <summary>
    /// Узел, с которым общаемся
    /// </summary>
    private readonly IPeer _peer;

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

    public ThreadPeerProcessor(IPeer peer, LeaderState<TCommand, TResponse> caller)
    {
        _thread = new Thread(ThreadWorker);
        var handle = new AutoResetEvent(false);
        _queue = new RequestQueue(handle);
        _queueStopEvent = handle;
        _peer = peer;
        _caller = caller;
    }

    public void Start(CancellationToken token)
    {
        _token = token;
        _thread.Start();
    }

    public void Replicate(LogReplicationRequest request)
    {
        _queue.Add(request);
    }

    private void ThreadWorker()
    {
        /*
         * Обновление состояния нужно делать не через этот поток,
         * т.к. это вызовет дедлок:
         * - Вызываем TryUpdateState
         * - TryUpdateState вызывает Dispose у нашего состояния лидера
         * - Состояние лидера вызывает Join для каждого потока
         * - Но т.к. вызвали мы, то получается вызываем Join для самих себя
         */

        var peerInfo = new PeerInfo(PersistenceFacade.LastEntry.Index + 1);
        Term? foundGreaterTerm = null;
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
                if (TryReplicateLog(request.LogIndex, peerInfo) is { } greaterTerm)
                {
                    request.NotifyFoundGreaterTerm(greaterTerm);
                    foundGreaterTerm = greaterTerm;
                }
                else
                {
                    request.NotifyComplete();
                }
            }
            else if (heartbeatOrRequest.TryGetHeartbeat(out var heartbeat))
            {
                // TODO: выставить null для поля Heartbeat синхронизатора после операции

                try
                {
                    if (TryReplicateLog(peerInfo.NextIndex, peerInfo) is { } greaterTerm)
                    {
                        heartbeat.NotifyFoundGreaterTerm(greaterTerm);
                        foundGreaterTerm = greaterTerm;
                    }
                    else
                    {
                        heartbeat.NotifySuccess();
                    }
                }
                finally
                {
                    heartbeat.Dispose();
                }
            }
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
                if (PersistenceFacade.TryGetSnapshot(out var snapshot))
                {
                    var lastEntry = PersistenceFacade.SnapshotStorage.LastLogEntry;
                    var installSnapshotResponse = _peer.SendInstallSnapshot(new InstallSnapshotRequest(CurrentTerm,
                        ConsensusModule.Id, lastEntry.Index, lastEntry.Term,
                        snapshot), _token);
                    if (installSnapshotResponse is null)
                    {
                        // Лучше не использовать ISnapshot несколько раз 
                        // Сделаем еще одну итерацию
                        continue;
                    }

                    if (CurrentTerm < installSnapshotResponse.CurrentTerm)
                    {
                        // У узла больший терм - уведомляем об этом
                        return installSnapshotResponse.CurrentTerm;
                    }

                    info.Update(1);
                    continue;
                }

                throw new ApplicationException(
                    $"Для репликации нужны данные лога с {info.NextIndex} индекса, но ни в логе ни в снапшоте этого лога нет");
            }

            // 1. Отправляем запрос с текущим отслеживаемым индексом узла
            var appendEntriesRequest = new AppendEntriesRequest(Term: CurrentTerm,
                LeaderCommit: PersistenceFacade.CommitIndex,
                LeaderId: ConsensusModule.Id,
                PrevLogEntryInfo: PersistenceFacade.GetPrecedingEntryInfo(info.NextIndex),
                Entries: entries);

            AppendEntriesResponse response;
            while (true)
            {
                var currentResponse = _peer.SendAppendEntriesAsync(appendEntriesRequest, _token)
                                            // TODO: заменить на синхронную версию
                                           .GetAwaiter()
                                           .GetResult();

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
                // 3.2. Обновить matchIndex = новый nextIndex - 1
                info.Update(appendEntriesRequest.Entries.Count);

                // 3.3. Если лог не до конца был синхронизирован
                if (info.NextIndex < replicationIndex)
                {
                    // Заходим на новый круг и отправляем еще
                    continue;
                }

                // 3.4. Уведомляем об успешной отправке команды на узел
                return null;
            }

            // Дальше узел отказался принимать наш запрос (Success = false)
            // 4. Если вернувшийся терм больше нашего
            if (CurrentTerm < response.Term)
            {
                // Уведосмляем о большем терме. 
                // Обновление состояние произойдет позже
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
}