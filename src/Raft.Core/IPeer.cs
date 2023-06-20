using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;

namespace Raft.Core;

/// <summary>
/// Интерфейс, представляющий другой узел 
/// </summary>
public interface IPeer
{
    /// <summary>
    /// Идентификатор узла
    /// </summary>
    public NodeId Id { get; }

    public Task<AppendEntriesResponse?> SendAppendEntries(AppendEntriesRequest request, CancellationToken token);
    
    /// <summary>
    /// Отправить запрос RequestVote указанному узлу
    /// </summary>
    /// <param name="request">Данные для запроса</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Ответ сервера, или <c>null</c> если ответа нет (например, таймаут из-за проблем сети)</returns>
    public Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token);
}