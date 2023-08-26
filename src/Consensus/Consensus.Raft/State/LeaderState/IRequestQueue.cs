namespace Consensus.Raft.State.LeaderState;

/// <summary>
/// Очередь команд на отправку AppendEntries.
/// Используется для передачи запросов обработчикам узлов в линейном порядке
/// </summary>
public interface IRequestQueue
{
    /// <summary>
    /// Получать все приходящие команды в потоке
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns>Асинхронный поток команд</returns>
    /// <remarks>Вызывается читателем - <see cref="PeerProcessor{TCommand,TResponse}"/></remarks>
    public IAsyncEnumerable<AppendEntriesRequestSynchronizer> ReadAllRequestsAsync(CancellationToken token);
    
    /// <summary>
    /// Послать команду на отправку Heartbeat
    /// </summary>
    /// <remarks>Вызывается при срабатывании Heartbeat Timeout</remarks>
    public void AddHeartbeatIfEmpty();
    
    /// <summary>
    /// Добавить в очередь <paramref name="synchronizer"/>
    /// </summary>
    /// <param name="synchronizer"><see cref="AppendEntriesRequestSynchronizer"/></param>
    public void AddAppendEntries(AppendEntriesRequestSynchronizer synchronizer);
}