using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using TaskFlux.Models;

namespace Consensus.Raft;

/// <summary>
/// Интерфейс, представляющий другой узел
/// </summary>
public interface IPeer
{
    /// <summary>
    /// Идентификатор узла
    /// </summary>
    public NodeId Id { get; }

    public Task<AppendEntriesResponse?> SendAppendEntriesAsync(AppendEntriesRequest request, CancellationToken token);
    public AppendEntriesResponse? SendAppendEntries(AppendEntriesRequest request);

    /// <summary>
    /// Отправить запрос RequestVote указанному узлу
    /// </summary>
    /// <param name="request">Данные запроса</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Ответ сервера, или <c>null</c> если ответа нет (например, таймаут из-за проблем сети)</returns>
    public Task<RequestVoteResponse?> SendRequestVoteAsync(RequestVoteRequest request, CancellationToken token);

    public RequestVoteResponse? SendRequestVote(RequestVoteRequest request);

    public IEnumerable<InstallSnapshotResponse?> SendInstallSnapshot(InstallSnapshotRequest request,
                                                                     CancellationToken token);
}