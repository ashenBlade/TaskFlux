using Raft.Core.Commands;

namespace Raft.Core.Peer;

/// <summary>
/// Интерфейс, представляющий другой узел 
/// </summary>
public interface IPeer
{
    /// <summary>
    /// Идентификатор узла
    /// </summary>
    public PeerId Id { get; }

    public Task<HeartbeatResponse?> SendHeartbeat(HeartbeatRequest request, CancellationToken token);
    
    /// <summary>
    /// Отправить запрос RequestVote указанному узлу
    /// </summary>
    /// <param name="request">Данные для запроса</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Ответ сервера, или <c>null</c> если ответа нет</returns>
    public Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token);
    public Task SendAppendEntries(CancellationToken token);
}