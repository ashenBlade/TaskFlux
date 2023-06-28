namespace Raft.Core.Node.LeaderState;

/// <summary>
/// Очередь команд на отправку AppendEntries.
/// Используется для передачи запросов обработчикам узлов в линейном порядке
/// </summary>
internal interface IRequestQueue
{
    /// <summary>
    /// Получать все приходящие команды в потоке
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns>Асинхронный поток команд</returns>
    /// <remarks>Вызывается читателем - <see cref="PeerProcessor"/></remarks>
    public IAsyncEnumerable<AppendEntriesRequestSynchronizer> ReadAllRequestsAsync(CancellationToken token);
    /// <summary>
    /// Послать команду на отправку Heartbeat
    /// </summary>
    /// <remarks>Вызывается при срабатывании Heartbeat Timeout</remarks>
    public void AddHeartbeat();
    
    /// <summary>
    /// Добавить в очередь <paramref name="synchronizer"/>
    /// </summary>
    /// <param name="synchronizer"><see cref="AppendEntriesRequestSynchronizer"/></param>
    public void AddAppendEntries(AppendEntriesRequestSynchronizer synchronizer);
}