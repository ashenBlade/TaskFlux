namespace Consensus.Raft.State.LeaderState;

/// <summary>
/// Специальная очередь для синхронизации обработчика узла и <see cref="LeaderState{TCommand,TResponse}"/> 
/// </summary>
internal class RequestQueue : IDisposable
{
    // Очередь для запросов
    private readonly Queue<HeartbeatOrRequest> _queue = new();
    private readonly AutoResetEvent _enqueueEvent = new(false);
    private readonly WaitHandle _processorLife;


    public RequestQueue(WaitHandle processorLife)
    {
        _processorLife = processorLife;
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
        var buffer = new[] {token.WaitHandle, _processorLife, _enqueueEvent,};

        while (!token.IsCancellationRequested)
        {
            var index = WaitHandle.WaitAny(buffer);
            if (index == 0)
            {
                // Токен отменен - пусть разберутся с остальным
                foreach (var i in _queue)
                {
                    yield return i;
                }

                yield break;
            }

            if (index == 1)
            {
                // Объект обработчика закрывается.
                // Нормально такого быть не может - закрыть сначала нужно 
                yield break;
            }

            var hasData = false;
            while (_queue.TryDequeue(out var data))
            {
                yield return data;
                hasData = true;
            }

            if (hasData)
            {
                continue;
            }

            yield return HeartbeatOrRequest.Heartbeat;
        }
    }

    /// <summary>
    /// Добавить в очередь запросов запрос на репликацию определенного индекса лога
    /// </summary>
    /// <param name="request">Запрос, который нужно сделать</param>
    public void Add(LogReplicationRequest request)
    {
        /*
         * Синхронная реализация очереди допустима, т.к.
         * обработка запросов SubmitRequest в приложении последовательная,
         * а значит и SubmitRequest вызывается последовательно
         */
        _queue.Enqueue(HeartbeatOrRequest.Request(request));
        _enqueueEvent.Set();
    }

    public void AddHeartbeat()
    {
        if (_queue.Count > 0)
        {
            // Очередь не пуста - есть элементы
            return;
        }

        _enqueueEvent.Set();
    }

    public void Dispose()
    {
        _enqueueEvent.Dispose();
    }
}