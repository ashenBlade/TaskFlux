using System.Diagnostics;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Commands.Submit;
using Consensus.Raft.Persistence;
using Consensus.Raft.State;
using Consensus.Raft.State.LeaderState;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Raft;

[DebuggerDisplay("Роль: {CurrentRole}; Терм: {CurrentTerm}; Id: {Id}")]
public class RaftConsensusModule<TCommand, TResponse>
    : IConsensusModule<TCommand, TResponse>,
      IDisposable
{
    private readonly ITimerFactory _timerFactory;
    private readonly ICommandSerializer<TCommand> _commandSerializer;

    public NodeRole CurrentRole =>
        ( ( IConsensusModule<TCommand, TResponse> ) this ).CurrentState.Role;

    private readonly ILogger _logger;
    public NodeId Id { get; }

    public Term CurrentTerm => PersistenceFacade.CurrentTerm;
    public NodeId? VotedFor => PersistenceFacade.VotedFor;
    public PeerGroup PeerGroup { get; }
    public IStateMachine<TCommand, TResponse> StateMachine { get; set; }
    private IStateMachineFactory<TCommand, TResponse> StateMachineFactory { get; }

    // Инициализируем либо в .Create (прод), либо через internal метод SetStateTest
    private State<TCommand, TResponse> _currentState = null!;

    State<TCommand, TResponse> IConsensusModule<TCommand, TResponse>.CurrentState => GetCurrentStateCheck();

    private State<TCommand, TResponse> GetCurrentStateCheck()
    {
        return _currentState
            ?? throw new ArgumentNullException(nameof(_currentState), "Текущее состояние еще не проставлено");
    }

    internal void SetStateTest(State<TCommand, TResponse> state)
    {
        // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
        if (_currentState is not null)
        {
            throw new InvalidOperationException($"Состояние узла уже выставлено в {_currentState.Role}");
        }

        state.Initialize();
        _currentState = state;
    }

    public bool TryUpdateState(State<TCommand, TResponse> newState,
                               State<TCommand, TResponse> oldState)
    {
        var stored = Interlocked.CompareExchange(ref _currentState, newState, oldState);
        if (stored == oldState)
        {
            stored.Dispose();
            newState.Initialize();
            RoleChanged?.Invoke(stored.Role, newState.Role);
            return true;
        }

        return false;
    }

    public IBackgroundJobQueue BackgroundJobQueue { get; }
    public StoragePersistenceFacade PersistenceFacade { get; }

    internal RaftConsensusModule(
        NodeId id,
        PeerGroup peerGroup,
        ILogger logger,
        ITimerFactory timerFactory,
        IBackgroundJobQueue backgroundJobQueue,
        StoragePersistenceFacade persistenceFacade,
        IStateMachine<TCommand, TResponse> stateMachine,
        ICommandSerializer<TCommand> commandSerializer,
        IStateMachineFactory<TCommand, TResponse> stateMachineFactory)
    {
        _timerFactory = timerFactory;
        _commandSerializer = commandSerializer;
        StateMachineFactory = stateMachineFactory;
        Id = id;
        _logger = logger;
        PeerGroup = peerGroup;
        BackgroundJobQueue = backgroundJobQueue;
        PersistenceFacade = persistenceFacade;
        StateMachine = stateMachine;
    }


    public RequestVoteResponse Handle(RequestVoteRequest request)
    {
        return _currentState.Apply(request);
    }

    public AppendEntriesResponse Handle(AppendEntriesRequest request)
    {
        return _currentState.Apply(request);
    }

    public IEnumerable<InstallSnapshotResponse> Handle(InstallSnapshotRequest request,
                                                       CancellationToken token = default)
    {
        return _currentState.Apply(request, token);
    }

    public SubmitResponse<TResponse> Handle(SubmitRequest<TCommand> request, CancellationToken token = default)
    {
        return _currentState.Apply(request, token);
    }

    public event RoleChangedEventHandler? RoleChanged;

    public State<TCommand, TResponse> CreateFollowerState()
    {
        return new FollowerState<TCommand, TResponse>(this, StateMachineFactory, _commandSerializer,
            _timerFactory.CreateElectionTimer(),
            _logger.ForContext("SourceContext", "Raft(Follower)"));
    }

    public State<TCommand, TResponse> CreateLeaderState()
    {
        return new LeaderState<TCommand, TResponse>(this, _logger.ForContext("SourceContext", "Raft(Leader)"),
            _commandSerializer, _timerFactory);
    }

    public State<TCommand, TResponse> CreateCandidateState()
    {
        return new CandidateState<TCommand, TResponse>(this, _timerFactory.CreateElectionTimer(),
            _logger.ForContext("SourceContext", "Raft(Candidate)"));
    }

    public override string ToString()
    {
        return $"RaftNode(Id = {Id}, Role = {CurrentRole}, Term = {CurrentTerm}, VotedFor = {VotedFor?.ToString() ?? "null"})";
    }

    public void Dispose()
    {
        _currentState.Dispose();
    }

    public static RaftConsensusModule<TCommand, TResponse> Create(
        NodeId id,
        PeerGroup peerGroup,
        ILogger logger,
        ITimerFactory timerFactory,
        IBackgroundJobQueue backgroundJobQueue,
        StoragePersistenceFacade persistenceFacade,
        IStateMachine<TCommand, TResponse> stateMachine,
        IStateMachineFactory<TCommand, TResponse> stateMachineFactory,
        ICommandSerializer<TCommand> commandSerializer)
    {
        var module = new RaftConsensusModule<TCommand, TResponse>(id, peerGroup, logger, timerFactory,
            backgroundJobQueue, persistenceFacade, stateMachine, commandSerializer, stateMachineFactory);
        var followerState = module.CreateFollowerState();
        module._currentState = followerState;
        followerState.Initialize();
        return module;
    }
}