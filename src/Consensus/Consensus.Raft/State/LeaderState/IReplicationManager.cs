namespace Consensus.Raft.State.LeaderState;

public interface IReplicationManager
{
    /// <summary>
    /// Метод триггер для отправки следующей Heartbeat команды узлам
    /// </summary>
    public void NotifyHeartbeat();

    /// <summary>
    /// Отправить команды всем узлам для репликации лога до указанного индекса.
    /// Метод нужен для репликации всех 
    /// </summary>
    /// <param name="index"></param>
    /// <param name="token"></param>
    public void Replicate(int index, CancellationToken token = default);
}