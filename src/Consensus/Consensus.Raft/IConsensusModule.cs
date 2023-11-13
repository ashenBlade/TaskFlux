using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Commands.Submit;
using Consensus.Raft.Persistence;
using Consensus.Raft.State;
using TaskFlux.Models;

namespace Consensus.Raft;

public interface IConsensusModule<TCommand, TResponse>
{
    /// <summary>
    /// ID текущего узла
    /// </summary>
    public NodeId Id { get; }

    /// <summary>
    /// Текущая роль узла
    /// </summary>
    public NodeRole CurrentRole => CurrentState.Role;

    /// <summary>
    /// Номер текущего терма
    /// </summary>
    public Term CurrentTerm { get; }

    /// <summary>
    /// Id кандидата, за которого проголосовала текущая нода
    /// </summary>
    public NodeId? VotedFor { get; }

    /// <summary>
    /// Текущее состояние узла в зависимости от роли: Follower, Candidate, Leader
    /// </summary>
    public State<TCommand, TResponse> CurrentState { get; }

    /// <summary>
    /// Заменить роль с <paramref name="oldState"/> на <paramref name="newState"/> с проверкой.
    /// Если предыдущее состояние было <paramref name="oldState"/>, то оно заменяется на новое <paramref name="newState"/>.
    /// Так же для нового состояния вызывается <see cref="State{TCommand,TResponse}.Initialize"/>,
    /// а для старого <see cref="State{TCommand,TResponse}.Dispose"/>.
    /// </summary>
    /// <param name="newState">Новое состояние</param>
    /// <param name="oldState">Старое состояние</param>
    public bool TryUpdateState(State<TCommand, TResponse> newState, State<TCommand, TResponse> oldState);

    /// <summary>
    /// Очередь задач для выполнения в на заднем фоне
    /// </summary>
    IBackgroundJobQueue BackgroundJobQueue { get; }

    /// <summary>
    /// Фасад для работы с файлами
    /// </summary>
    StoragePersistenceFacade PersistenceFacade { get; }

    /// <summary>
    /// Группа других узлов кластера
    /// </summary>
    public PeerGroup PeerGroup { get; }

    /// <summary>
    /// Объект, представляющий текущий узел
    /// </summary>
    public IApplication<TCommand, TResponse> Application { get; set; }

    public RequestVoteResponse Handle(RequestVoteRequest request) => CurrentState.Apply(request);
    public AppendEntriesResponse Handle(AppendEntriesRequest request) => CurrentState.Apply(request);

    public SubmitResponse<TResponse> Handle(SubmitRequest<TCommand> request, CancellationToken token = default) =>
        CurrentState.Apply(request, token);

    public IEnumerable<InstallSnapshotResponse> Handle(InstallSnapshotRequest request,
                                                       CancellationToken token = default) =>
        CurrentState.Apply(request, token);

    /// <summary>
    /// Событие, вызывающееся при обновлении роли узла.
    /// </summary>
    public event RoleChangedEventHandler RoleChanged;

    public State<TCommand, TResponse> CreateFollowerState();
    public State<TCommand, TResponse> CreateLeaderState();
    public State<TCommand, TResponse> CreateCandidateState();
}