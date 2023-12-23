using Consensus.Core;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Persistence;
using Consensus.Raft.State;
using TaskFlux.Models;

namespace Consensus.Raft;

/// <summary>
/// Модуль консенсуса, отвечающий за принятие запросов и решение как их обрабатывать.
/// Логика зависит от роли текущего узла.
/// Используется, когда приложение запускается в кластере.
/// </summary>
/// <typeparam name="TCommand">Класс команды</typeparam>
/// <typeparam name="TResponse">Класс результата выполнения команды</typeparam>
public interface IRaftConsensusModule<TCommand, TResponse>
    : IConsensusModule<TCommand, TResponse>
{
    /// <summary>
    /// ID текущего узла
    /// </summary>
    public NodeId Id { get; }

    /// <summary>
    /// Текущая роль узла
    /// </summary>
    // public NodeRole CurrentRole => CurrentState.Role;
    public NodeRole CurrentRole { get; }

    /// <summary>
    /// Номер текущего терма
    /// </summary>
    public Term CurrentTerm { get; }

    /// <summary>
    /// Id кандидата, за которого проголосовала текущая нода
    /// </summary>
    public NodeId? VotedFor { get; }

    /// <summary>
    /// ID известного лидера кластера
    /// </summary>
    public NodeId? LeaderId { get; }

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
    /// Фабрика для создания новых приложений
    /// </summary>
    public IApplicationFactory<TCommand, TResponse> ApplicationFactory { get; }

    public RequestVoteResponse Handle(RequestVoteRequest request);
    public AppendEntriesResponse Handle(AppendEntriesRequest request);

    public IEnumerable<InstallSnapshotResponse> Handle(InstallSnapshotRequest request, CancellationToken token);

    public State<TCommand, TResponse> CreateFollowerState();
    public State<TCommand, TResponse> CreateLeaderState();
    public State<TCommand, TResponse> CreateCandidateState();
}