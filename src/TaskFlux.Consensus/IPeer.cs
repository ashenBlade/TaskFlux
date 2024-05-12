using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Core;

namespace TaskFlux.Consensus;

/// <summary>
/// Интерфейс, представляющий другой узел
/// </summary>
public interface IPeer
{
    /// <summary>
    /// Идентификатор узла
    /// </summary>
    public NodeId Id { get; }

    /// <summary>
    /// Отправить запрос для добавления новых записей в лог
    /// </summary>
    /// <param name="request">Запрос для добавления записей</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Результат добавления записей</returns>
    public AppendEntriesResponse SendAppendEntries(AppendEntriesRequest request, CancellationToken token);

    /// <summary>
    /// Отправить запрос на получение голоса указанному узлу
    /// </summary>
    /// <param name="request">Запрос получения голоса</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Полученный ответ</returns>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    public RequestVoteResponse SendRequestVote(RequestVoteRequest request, CancellationToken token);

    /// <summary>
    /// Отправить запрос на установку снапшота
    /// </summary>
    /// <param name="request">Запрос для установки снапшота</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Результат установки снапшота</returns>
    public InstallSnapshotResponse SendInstallSnapshot(InstallSnapshotRequest request, CancellationToken token);
}