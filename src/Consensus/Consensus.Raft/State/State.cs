using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Commands.Submit;
using Consensus.Raft.Persistence;
using TaskFlux.Models;

namespace Consensus.Raft.State;

/// <summary>
/// Интерфейс, представляющий конкретное состояние узла
/// </summary>
/// <remarks>
/// IDisposable нужно вызывать для сброса таймеров и очистки ресурсов предыдущего состояния (отписка от таймеров и т.д.)
/// </remarks>
public abstract class State<TCommand, TResponse>
{
    internal IRaftConsensusModule<TCommand, TResponse> RaftConsensusModule { get; }
    protected StoragePersistenceFacade PersistenceFacade => RaftConsensusModule.PersistenceFacade;
    protected Term CurrentTerm => RaftConsensusModule.CurrentTerm;
    protected NodeId? VotedFor => RaftConsensusModule.VotedFor;

    protected IApplication<TCommand, TResponse> Application
    {
        get => RaftConsensusModule.Application;
        set => RaftConsensusModule.Application = value
                                              ?? throw new ArgumentNullException(nameof(Application),
                                                     "Попытка установить новое состояние в null");
    }

    protected NodeId Id => RaftConsensusModule.Id;
    protected IBackgroundJobQueue BackgroundJobQueue => RaftConsensusModule.BackgroundJobQueue;
    protected PeerGroup PeerGroup => RaftConsensusModule.PeerGroup;

    internal State(IRaftConsensusModule<TCommand, TResponse> raftConsensusModule)
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
    /// <param name="request">Объект запроса</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Результат операции</returns>
    public abstract SubmitResponse<TResponse> Apply(SubmitRequest<TCommand> request, CancellationToken token = default);

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
    public abstract IEnumerable<InstallSnapshotResponse> Apply(InstallSnapshotRequest request,
                                                               CancellationToken token = default);
}