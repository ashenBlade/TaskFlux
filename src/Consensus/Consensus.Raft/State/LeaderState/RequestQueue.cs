namespace Consensus.Raft.State.LeaderState;

/// <summary>
/// Специальная очередь для синхронизации обработчика узла и <see cref="LeaderState{TCommand,TResponse}"/> 
/// </summary>
internal class RequestQueue : IDisposable
{
    // Очередь для запросов
    private readonly Queue<HeartbeatOrRequest> _queue = new();

    // Объект синхронизатора для Heartbeat запроса
    private HeartbeatSynchronizer? _heartbeatSynchronizer = null;

    // Сигнал того, что в очередь добавили элемент
    private readonly AutoResetEvent _enqueueEvent = new(false);

    // Сигнал того, что приложение закрывается
    private readonly WaitHandle _processLife;

    private volatile bool _disposed;

    public RequestQueue(WaitHandle processLife)
    {
        _processLife = processLife;
    }

    /// <summary>
    /// Метод будет получать все переданные запросы на отправку.
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <remarks>
    /// Единственный способ остановить работу - отменить токен,
    /// поэтому токен нужно передавать корректный (не заглушку)
    /// </remarks>
    /// <returns>Поток объектов запросов, передаваемых от лидера</returns>
    public IEnumerable<HeartbeatOrRequest> ReadAllRequests(CancellationToken token)
    {
        WaitHandle[] buffer;
        try
        {
            buffer = new[]
            {
                token.WaitHandle, // Если его отменят до момента вызова WaitHandle.WaitAny, то мы просто выйдем сразу
                _processLife, _enqueueEvent,
            };
        }
        catch (ObjectDisposedException)
        {
            // Мы стали лидером, но внезапно получили больший терм,
            // поэтому CancellationTokenSource был сброшен
            yield break;
        }

        while (!token.IsCancellationRequested)
        {
            int index;
            try
            {
                index = WaitHandle.WaitAny(buffer);
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            if (index == 0)
            {
                break;
            }

            if (index == 1)
            {
                break;
            }

            while (_queue.TryDequeue(out var data))
            {
                yield return data;
            }

            if (Interlocked.CompareExchange(ref _heartbeatSynchronizer, null, _heartbeatSynchronizer) is
                { } synchronizer)
            {
                yield return HeartbeatOrRequest.Heartbeat(synchronizer);
            }
        }

        // Вместо сложной проверки отмены токена внутри цикла, 
        // просто вернем оставшиеся данные
        while (_queue.TryDequeue(out var data))
        {
            yield return data;
        }
    }

    /// <summary>
    /// Добавить в очередь запросов запрос на репликацию определенного индекса лога
    /// </summary>
    /// <param name="request">Запрос, который нужно сделать</param>
    public bool Add(LogReplicationRequest request)
    {
        if (_disposed)
        {
            return false;
        }

        /*
         * Синхронная реализация очереди допустима, т.к.
         * обработка запросов SubmitRequest в приложении последовательная,
         * а значит и SubmitRequest вызывается последовательно
         */
        _queue.Enqueue(HeartbeatOrRequest.Request(request));
        _enqueueEvent.Set();
        return true;
    }

    public bool TryAddHeartbeat(out HeartbeatSynchronizer synchronizer)
    {
        if (_disposed)
        {
            synchronizer = default!;
            return false;
        }

        if (0 < _queue.Count)
        {
            // Очередь не пуста - есть запросы пользователей
            synchronizer = default!;
            return false;
        }

        if (_heartbeatSynchronizer is null)
        {
            var sync = new HeartbeatSynchronizer();
            var stored = Interlocked.CompareExchange(ref _heartbeatSynchronizer, sync, null);
            if (stored == null)
            {
                // Мы добавили новый Synchronizer
                try
                {
                    _enqueueEvent.Set();
                }
                catch (ObjectDisposedException)
                {
                    Interlocked.CompareExchange(ref _heartbeatSynchronizer, null, sync);
                    sync.Dispose();
                    synchronizer = default!;
                    return false;
                }

                synchronizer = sync;
                return true;
            }

            // Не удалось добавить - новый уже кто-то поставил
            sync.Dispose();
        }

        synchronizer = default!;
        return false;
    }

    public void Dispose()
    {
        _disposed = true;
        _enqueueEvent.Dispose();
    }
}