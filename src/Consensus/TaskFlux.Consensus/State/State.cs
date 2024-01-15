using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Core;

namespace TaskFlux.Consensus.State;

/// <summary>
/// Интерфейс, представляющий конкретное состояние узла
/// </summary>
/// <remarks>
/// IDisposable нужно вызывать для сброса таймеров и очистки ресурсов предыдущего состояния (отписка от таймеров и т.д.)
/// </remarks>
public abstract class State<TCommand, TResponse>
{
    internal RaftConsensusModule<TCommand, TResponse> RaftConsensusModule { get; }
    protected StoragePersistenceFacade PersistenceFacade => RaftConsensusModule.PersistenceFacade;
    protected Term CurrentTerm => RaftConsensusModule.CurrentTerm;
    protected NodeId? VotedFor => RaftConsensusModule.VotedFor;
    protected NodeId Id => RaftConsensusModule.Id;
    protected IBackgroundJobQueue BackgroundJobQueue => RaftConsensusModule.BackgroundJobQueue;
    protected IApplicationFactory<TCommand, TResponse> ApplicationFactory => RaftConsensusModule.ApplicationFactory;
    protected PeerGroup PeerGroup => RaftConsensusModule.PeerGroup;

    internal State(RaftConsensusModule<TCommand, TResponse> raftConsensusModule)
    {
        RaftConsensusModule = raftConsensusModule;
    }

    /// <summary>
    /// Метод для инициализации состояния: регистрация обработчиков, запуск фоновых процессов
    /// </summary>
    public abstract void Initialize();

    /// <summary>
    /// Текущая роль этого состояния
    /// </summary>
    public abstract NodeRole Role { get; }

    /// <summary>
    /// ID известного лидера
    /// </summary>
    public abstract NodeId? LeaderId { get; }

    /// <summary>
    /// Применить команду RequestVote
    /// </summary>
    /// <param name="request">Объект запроса</param>
    /// <returns>Ответ узла</returns>
    public abstract RequestVoteResponse Apply(RequestVoteRequest request);

    /// <summary>
    /// Применить команду AppendEntries
    /// </summary>
    /// <param name="request">Объект запроса</param>
    /// <returns>Ответ узла</returns>
    public abstract AppendEntriesResponse Apply(AppendEntriesRequest request);

    /// <summary>
    /// Применить команду к приложению
    /// </summary>
    /// <param name="command">Объект запроса</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Результат операции</returns>
    public abstract SubmitResponse<TResponse> Apply(TCommand command, CancellationToken token = default);

    /// <summary>
    /// Вызывается, когда состояние узла меняется, для очищения предыдущего (т.е. состояния, которому этот Dispose принадлежит) состояния
    /// </summary>
    /// <example>
    /// Сработал ElectionTimeout и из Follower узел перешел в Candidate
    /// </example>
    public abstract void Dispose();

    /// <summary>
    /// Применить команду InstallSnapshot для создания нового снапшота, принятого от лидера.
    /// </summary>
    /// <param name="request"><see cref="InstallSnapshotRequest"/></param>
    /// <param name="token">Токен отмены</param>
    /// <returns><see cref="InstallSnapshotResponse"/></returns>
    public abstract InstallSnapshotResponse Apply(InstallSnapshotRequest request, CancellationToken token = default);
}