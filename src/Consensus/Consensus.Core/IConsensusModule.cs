using Consensus.CommandQueue;
using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.InstallSnapshot;
using Consensus.Core.Commands.RequestVote;
using Consensus.Core.Commands.Submit;
using Consensus.Core.Log;
using Consensus.Core.State;
using Consensus.Core.State.LeaderState;
using TaskFlux.Core;

namespace Consensus.Core;

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
    public ConsensusModuleState<TCommand, TResponse> CurrentState { get; set; }

    /// <summary>
    /// Заменить роль с <paramref name="oldState"/> на <paramref name="newState"/> с проверкой.
    /// Если предыдущее состояние было <paramref name="oldState"/>, то оно заменяется на новое <paramref name="newState"/>.
    /// Так же для нового состояния вызывается <see cref="ConsensusModuleState{TCommand,TResponse}.Initialize"/>,
    /// а для старого <see cref="ConsensusModuleState{TCommand,TResponse}.Dispose"/>.
    /// </summary>
    /// <param name="newState">Новое состояние</param>
    /// <param name="oldState">Старое состояние</param>
    public bool TryUpdateState(ConsensusModuleState<TCommand, TResponse> newState,
                               ConsensusModuleState<TCommand, TResponse> oldState);

    /// <summary>
    /// Таймер выборов.
    /// Используется в Follower и Candidate состояниях
    /// </summary>
    ITimer ElectionTimer { get; }

    /// <summary>
    /// Таймер для отправки Heartbeat запросов
    /// </summary>
    ITimer HeartbeatTimer { get; }

    /// <summary>
    /// Очередь задач для выполнения в на заднем фоне
    /// </summary>
    IBackgroundJobQueue BackgroundJobQueue { get; }

    /// <summary>
    /// Очередь команд для применения к машине состояний.
    /// Если нужно применить команду (бизнес-логики), то это сюда, а не напрямую в <see cref="StateMachine"/>
    /// </summary>
    ICommandQueue CommandQueue { get; }

    /// <summary>
    /// Фасад для работы с файлами
    /// </summary>
    IPersistenceManager PersistenceManager { get; }

    /// <summary>
    /// Группа других узлов кластера
    /// </summary>
    public PeerGroup PeerGroup { get; }

    /// <summary>
    /// Объект, представляющий текущий узел
    /// </summary>
    public IStateMachine<TCommand, TResponse> StateMachine { get; set; }

    public IStateMachineFactory<TCommand, TResponse> StateMachineFactory { get; }
    public ISerializer<TCommand> CommandSerializer { get; }
    public RequestVoteResponse Handle(RequestVoteRequest request);
    public AppendEntriesResponse Handle(AppendEntriesRequest request);
    public SubmitResponse<TResponse> Handle(SubmitRequest<TCommand> request);
    public InstallSnapshotResponse Handle(InstallSnapshotRequest request, CancellationToken token);

    /// <summary>
    /// Событие, вызывающееся при обновлении роли узла
    /// </summary>
    public event RoleChangedEventHandler RoleChanged;

    public FollowerState<TCommand, TResponse> CreateFollowerState();
    public LeaderState<TCommand, TResponse> CreateLeaderState();
    public CandidateState<TCommand, TResponse> CreateCandidateState();
}