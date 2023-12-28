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

    public AppendEntriesResponse SendAppendEntries(AppendEntriesRequest request, CancellationToken token);

    /// <summary>
    /// Отправить запрос RequestVote указанному узлу
    /// </summary>
    /// <param name="request">Данные запроса</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Ответ сервера, или <c>null</c> если ответа нет (например, таймаут из-за проблем сети)</returns>
    public Task<RequestVoteResponse?> SendRequestVoteAsync(RequestVoteRequest request, CancellationToken token);

    /// <summary>
    /// Отправить запрос на получение голоса указанному узлу
    /// </summary>
    /// <param name="request">Запрос получения голоса</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Полученный ответ</returns>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    public RequestVoteResponse SendRequestVote(RequestVoteRequest request, CancellationToken token);

    // TODO: возвращать не IEnumerable, а полноценный ответ типа: Ok, GreaterTerm и т.д.
    // так же с переповторами (как-нибудь это все надо обрабатывать)
    public IEnumerable<InstallSnapshotResponse?> SendInstallSnapshot(InstallSnapshotRequest request,
                                                                     CancellationToken token);
}