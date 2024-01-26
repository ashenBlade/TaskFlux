using System.Diagnostics;
using Serilog;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.State;
using TaskFlux.Consensus.State.LeaderState;
using TaskFlux.Core;

namespace TaskFlux.Consensus;

/// <summary>
/// Модуль консенсуса, отвечающий за принятие запросов и решение как их обрабатывать.
/// Логика зависит от роли текущего узла.
/// Используется, когда приложение запускается в кластере.
/// </summary>
/// <typeparam name="TCommand">Класс команды</typeparam>
/// <typeparam name="TResponse">Класс результата выполнения команды</typeparam>
[DebuggerDisplay("Роль: {CurrentRole}; Терм: {CurrentTerm}; Id: {Id}")]
public class RaftConsensusModule<TCommand, TResponse>
    : IConsensusModule<TCommand, TResponse>,
      IDisposable
{
    private readonly ITimerFactory _timerFactory;
    private readonly IDeltaExtractor<TResponse> _deltaExtractor;

    /// <summary>
    /// Текущая роль узла
    /// </summary>
    // public NodeRole CurrentRole => CurrentState.Role;
    public NodeRole CurrentRole => CurrentState.Role;

    private readonly ILogger _logger;

    /// <summary>
    /// ID текущего узла
    /// </summary>
    public NodeId Id { get; }

    /// <summary>
    /// Номер текущего терма
    /// </summary>
    public Term CurrentTerm => Persistence.CurrentTerm;

    /// <summary>
    /// Id кандидата, за которого проголосовала текущая нода
    /// </summary>
    public NodeId? VotedFor => Persistence.VotedFor;

    /// <summary>
    /// ID известного лидера кластера
    /// </summary>
    public NodeId? LeaderId => _currentState.LeaderId;

    /// <summary>
    /// Группа других узлов кластера
    /// </summary>
    public PeerGroup PeerGroup { get; }

    /// <summary>
    /// Фабрика для создания новых приложений
    /// </summary>
    public IApplicationFactory<TCommand, TResponse> ApplicationFactory { get; }

    // Инициализируем либо в .Create (прод), либо через internal метод SetStateTest
    private State<TCommand, TResponse> _currentState = null!;

    private State<TCommand, TResponse> CurrentState => GetCurrentStateCheck();

    private State<TCommand, TResponse> GetCurrentStateCheck()
    {
        return _currentState
            ?? throw new ArgumentNullException(nameof(_currentState), "Текущее состояние еще не проставлено");
    }

    internal void SetStateTest(State<TCommand, TResponse> state)
    {
        if (_currentState is not null)
        {
            throw new InvalidOperationException($"Состояние узла уже выставлено в {_currentState.Role}");
        }

        _currentState = state;
        state.Initialize();
    }

    /// <summary>
    /// Заменить роль с <paramref name="oldState"/> на <paramref name="newState"/> с проверкой.
    /// Если предыдущее состояние было <paramref name="oldState"/>, то оно заменяется на новое <paramref name="newState"/>.
    /// Так же для нового состояния вызывается <see cref="State{TCommand,TResponse}.Initialize"/>,
    /// а для старого <see cref="State{TCommand,TResponse}.Dispose"/>.
    /// </summary>
    /// <param name="newState">Новое состояние</param>
    /// <param name="oldState">Старое состояние</param>
    public bool TryUpdateState(State<TCommand, TResponse> newState,
                               State<TCommand, TResponse> oldState)
    {
        var stored = Interlocked.CompareExchange(ref _currentState, newState, oldState);
        if (stored == oldState)
        {
            stored.Dispose();
            newState.Initialize();
            return true;
        }

        return false;
    }

    /// <summary>
    /// Очередь задач для выполнения в на заднем фоне
    /// </summary>
    public IBackgroundJobQueue BackgroundJobQueue { get; }

    /// <summary>
    /// Фасад для работы с файлами
    /// </summary>
    public FileSystemPersistenceFacade Persistence { get; }

    internal RaftConsensusModule(
        NodeId id,
        PeerGroup peerGroup,
        ILogger logger,
        ITimerFactory timerFactory,
        IBackgroundJobQueue backgroundJobQueue,
        FileSystemPersistenceFacade persistence,
        IDeltaExtractor<TResponse> deltaExtractor,
        IApplicationFactory<TCommand, TResponse> applicationFactory)
    {
        _timerFactory = timerFactory;
        _deltaExtractor = deltaExtractor;
        Id = id;
        _logger = logger;
        PeerGroup = peerGroup;
        BackgroundJobQueue = backgroundJobQueue;
        Persistence = persistence;
        ApplicationFactory = applicationFactory;
    }


    public RequestVoteResponse Handle(RequestVoteRequest request)
    {
        return _currentState.Apply(request);
    }

    public AppendEntriesResponse Handle(AppendEntriesRequest request)
    {
        return _currentState.Apply(request);
    }

    public InstallSnapshotResponse Handle(InstallSnapshotRequest request, CancellationToken token = default)
    {
        return _currentState.Apply(request, token);
    }

    public SubmitResponse<TResponse> Handle(TCommand command, CancellationToken token = default)
    {
        return _currentState.Apply(command, token);
    }

    public State<TCommand, TResponse> CreateFollowerState()
    {
        return new FollowerState<TCommand, TResponse>(this,
            _timerFactory.CreateElectionTimer(),
            _logger.ForContext("SourceContext", "Raft(Follower)"));
    }

    public State<TCommand, TResponse> CreateLeaderState()
    {
        return new LeaderState<TCommand, TResponse>(this, _logger.ForContext("SourceContext", "Raft(Leader)"),
            _deltaExtractor, _timerFactory);
    }

    public State<TCommand, TResponse> CreateCandidateState()
    {
        return new CandidateState<TCommand, TResponse>(this, _timerFactory.CreateElectionTimer(),
            _logger.ForContext("SourceContext", "Raft(Candidate)"));
    }

    public override string ToString()
    {
        return
            $"RaftNode(Id = {Id}, Role = {CurrentRole}, Term = {CurrentTerm}, VotedFor = {VotedFor?.ToString() ?? "null"})";
    }

    public void Dispose()
    {
        _currentState.Dispose();
    }

    public void Start()
    {
        _currentState.Initialize();
    }

    public static RaftConsensusModule<TCommand, TResponse> Create(
        NodeId id,
        PeerGroup peerGroup,
        ILogger logger,
        ITimerFactory timerFactory,
        IBackgroundJobQueue backgroundJobQueue,
        FileSystemPersistenceFacade persistenceFacade,
        IDeltaExtractor<TResponse> deltaExtractor,
        IApplicationFactory<TCommand, TResponse> applicationFactory)
    {
        var module = new RaftConsensusModule<TCommand, TResponse>(id, peerGroup, logger, timerFactory,
            backgroundJobQueue, persistenceFacade, deltaExtractor, applicationFactory);
        var followerState = module.CreateFollowerState();
        module._currentState = followerState;

        // Инициализация должна быть в Start - запуск таймера выборов
        // followerState.Initialize();
        return module;
    }
}