using System.Diagnostics;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Persistence;

namespace Consensus.Raft.State.LeaderState;

public class ThreadPeerProcessor<TCommand, TResponse> : IDisposable
{
    private readonly RaftConsensusModule<TCommand, TResponse> _consensusModule;
    private readonly IPeer _peer;
    private Term CurrentTerm => _consensusModule.CurrentTerm;
    private StoragePersistenceFacade PersistenceFacade => _consensusModule.PersistenceFacade;
    private Thread? _thread;
    private readonly RequestQueue _queue;

    private readonly AutoResetEvent _stateChanged;
    private int _state;

    // Эти 2 поля обновляются при каждом вызове Start
    private LeaderState<TCommand, TResponse>? _caller = null;
    private CancellationToken _token;

    public ThreadPeerProcessor(IPeer peer,
                               RaftConsensusModule<TCommand, TResponse> consensusModule)
    {
        _consensusModule = consensusModule;
        _thread = new Thread(ThreadWorker);
        var handle = new AutoResetEvent(false);
        _queue = new RequestQueue(handle);
        _stateChanged = handle;

        _peer = peer;
    }

    public void Start(LeaderState<TCommand, TResponse> caller, CancellationToken token)
    {
        _caller = caller;
        _token = token;
        if (_thread is null)
        {
            _thread = new Thread(ThreadWorker);
        }

        if (!_thread.IsAlive)
        {
            _thread.Start(this);
        }
    }

    public void NotifyHeartbeat()
    {
        _queue.AddHeartbeat();
    }

    public void Replicate(LogReplicationRequest request)
    {
        _queue.Add(request);
    }

    private void ThreadWorker(object? obj)
    {
        // Буфер с дескрипторами для ожидания. 
        // Заполнится потом
        while (true)
        {
            // Ждем, сигнала к работе
            _stateChanged.WaitOne();
            if (_state == StateDisposed)
            {
                // Работа закончена
                break;
            }

            if (_state == StateBecomeLeader)
            {
                // Мы стали лидером - начинаем обрабатывать узел
                StartProcessingPeer();
            }
        }
    }

    private void StartProcessingPeer()
    {
        var peerInfo = new PeerInfo(PersistenceFacade.LastEntry.Index + 1);
        foreach (var obj in _queue.ReadAllRequests(_token))
        {
            if (_token.IsCancellationRequested && obj.TryGetRequest(out var r))
            {
                // Если работа закончена - оповестить всех о конце
                r.NotifyComplete();
                continue;
            }

            if (obj.TryGetRequest(out var request))
            {
                ReplicateLog(request.LogIndex, peerInfo);
                request.NotifyComplete();
            }
            else
            {
                ReplicateLog(peerInfo.NextIndex, peerInfo);
            }
        }
    }

    private void ReplicateLog(int replicationIndex, PeerInfo info)
    {
        while (true)
        {
            if (!PersistenceFacade.TryGetFrom(info.NextIndex, out var entries))
            {
                if (PersistenceFacade.TryGetSnapshot(out var snapshot))
                {
                    var lastEntry = PersistenceFacade.SnapshotStorage.LastLogEntry;
                    var installSnapshotResponse = _peer.SendInstallSnapshot(
                        new InstallSnapshotRequest(CurrentTerm, _consensusModule.Id, lastEntry.Index, lastEntry.Term,
                            snapshot), _token);
                    // Обновляем только на 1, чтобы начать отдавать уже из лога (текущий индекс находится уже в снапшоте)
                    if (CurrentTerm < installSnapshotResponse.CurrentTerm)
                    {
                        // У узла больший терм - переходим в последователя
                        var followerState = _consensusModule.CreateFollowerState();
                        Debug.Assert(_caller != null,
                            "Поле _caller не может быть null на момент работы внутри функции обработчика");
                        if (_consensusModule.TryUpdateState(followerState, _caller))
                        {
                            _consensusModule.PersistenceFacade.UpdateState(installSnapshotResponse.CurrentTerm, null);
                        }

                        return;
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
                LeaderId: _consensusModule.Id,
                PrevLogEntryInfo: PersistenceFacade.GetPrecedingEntryInfo(info.NextIndex),
                Entries: entries);

            AppendEntriesResponse response;
            while (true)
            {
                var currentResponse = _peer.SendAppendEntries(appendEntriesRequest, _token)
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
                return;
            }

            // Дальше узел отказался принимать наш запрос (Success = false)
            // 4. Если вернувшийся терм больше нашего
            if (CurrentTerm < response.Term)
            {
                // 4.1. Перейти в состояние Follower
                var followerState = _consensusModule.CreateFollowerState();
                Debug.Assert(_caller != null,
                    "Поле _caller не может быть null на момент работы внутри функции обработчика");
                if (_consensusModule.TryUpdateState(followerState, _caller))
                {
                    _consensusModule.PersistenceFacade.UpdateState(response.Term, null);
                }

                // 4.2. Закончить работу
                return;
            }

            // 5. В противном случае у узла не синхронизирован лог 

            // 5.1. Декрементируем последние записи лога
            // TODO: можно сразу вычислить место с которого нужно отправлять записи (его терм знаем)
            info.Decrement();

            // 5.2. Идем на следующий круг
        }
    }

    private const int StateBecomeLeader = 1;

    public void Dispose()
    {
        if (_thread is {IsAlive: true})
        {
            _state = StateDisposed;
            _stateChanged.Set();
            _thread.Join();
        }

        _queue.Dispose();
    }

    private const int StateDisposed = int.MinValue;
}