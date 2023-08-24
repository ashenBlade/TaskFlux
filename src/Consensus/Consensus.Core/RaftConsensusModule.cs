using System.Diagnostics;
using Consensus.CommandQueue;
using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.InstallSnapshot;
using Consensus.Core.Commands.RequestVote;
using Consensus.Core.Commands.Submit;
using Consensus.Core.Log;
using Consensus.Core.State;
using Consensus.Core.State.LeaderState;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Core;

[DebuggerDisplay("Роль: {CurrentRole}; Терм: {CurrentTerm}; Id: {Id}")]
public class RaftConsensusModule<TCommand, TResponse>
    : IConsensusModule<TCommand, TResponse>,
      IDisposable
{
    private readonly IRequestQueueFactory _requestQueueFactory;

    public NodeRole CurrentRole =>
        ( ( IConsensusModule<TCommand, TResponse> ) this ).CurrentState.Role;

    private ILogger Logger { get; }
    public NodeId Id { get; }
    public Term CurrentTerm => MetadataStorage.ReadTerm();
    public NodeId? VotedFor => MetadataStorage.ReadVotedFor();
    public PeerGroup PeerGroup { get; }
    public IStateMachine<TCommand, TResponse> StateMachine { get; set; }
    public IStateMachineFactory<TCommand, TResponse> StateMachineFactory { get; private set; }
    public IMetadataStorage MetadataStorage { get; }
    public ISerializer<TCommand> CommandSerializer { get; }

    // Выставляем вручную в .Create
    private ConsensusModuleState<TCommand, TResponse>? _currentState;

    ConsensusModuleState<TCommand, TResponse> IConsensusModule<TCommand, TResponse>.CurrentState
    {
        get => GetCurrentStateCheck();
        set
        {
            var oldState = _currentState;
            oldState?.Dispose();
            _currentState = value;
            value.Initialize();

            RoleChanged?.Invoke(oldState?.Role ?? NodeRole.Follower, value.Role);
        }
    }

    private ConsensusModuleState<TCommand, TResponse> GetCurrentStateCheck()
    {
        return _currentState
            ?? throw new ArgumentNullException(nameof(_currentState), "Текущее состояние еще не проставлено");
    }

    public ITimer ElectionTimer { get; }
    public ITimer HeartbeatTimer { get; }
    public IBackgroundJobQueue BackgroundJobQueue { get; }
    public ICommandQueue CommandQueue { get; }
    public IPersistenceManager PersistenceManager { get; }

    internal RaftConsensusModule(
        NodeId id,
        PeerGroup peerGroup,
        ILogger logger,
        ITimer electionTimer,
        ITimer heartbeatTimer,
        IBackgroundJobQueue backgroundJobQueue,
        IPersistenceManager persistenceManager,
        ICommandQueue commandQueue,
        IStateMachine<TCommand, TResponse> stateMachine,
        IMetadataStorage metadataStorage,
        ISerializer<TCommand> commandSerializer,
        IRequestQueueFactory requestQueueFactory,
        IStateMachineFactory<TCommand, TResponse> stateMachineFactory)
    {
        _requestQueueFactory = requestQueueFactory;
        StateMachineFactory = stateMachineFactory;
        Id = id;
        Logger = logger;
        PeerGroup = peerGroup;
        ElectionTimer = electionTimer;
        HeartbeatTimer = heartbeatTimer;
        BackgroundJobQueue = backgroundJobQueue;
        PersistenceManager = persistenceManager;
        CommandQueue = commandQueue;
        StateMachine = stateMachine;
        MetadataStorage = metadataStorage;
        CommandSerializer = commandSerializer;
    }

    public void UpdateState(Term newTerm, NodeId? votedFor)
    {
        MetadataStorage.Update(newTerm, votedFor);
    }

    public RequestVoteResponse Handle(RequestVoteRequest request)
    {
        return CommandQueue.Enqueue(new RequestVoteCommand<TCommand, TResponse>(request, this));
    }

    public AppendEntriesResponse Handle(AppendEntriesRequest request)
    {
        return CommandQueue.Enqueue(new AppendEntriesCommand<TCommand, TResponse>(request, this));
    }

    public InstallSnapshotResponse Handle(InstallSnapshotRequest request, CancellationToken token)
    {
        return CommandQueue.Enqueue(new InstallSnapshotCommand<TCommand, TResponse>(request, this, token));
    }

    public SubmitResponse<TResponse> Handle(SubmitRequest<TCommand> request)
    {
        return GetCurrentStateCheck().Apply(request);
    }

    public event RoleChangedEventHandler? RoleChanged;

    public FollowerState<TCommand, TResponse> CreateFollowerState()
    {
        return new FollowerState<TCommand, TResponse>(this, Logger.ForContext("SourceContext", "Raft(Follower)"));
    }

    public LeaderState<TCommand, TResponse> CreateLeaderState()
    {
        return new LeaderState<TCommand, TResponse>(this, Logger.ForContext("SourceContext", "Raft(Leader)"),
            _requestQueueFactory);
    }

    public CandidateState<TCommand, TResponse> CreateCandidateState()
    {
        return new CandidateState<TCommand, TResponse>(this, Logger.ForContext("SourceContext", "Raft(Candidate)"));
    }

    public override string ToString()
    {
        return
            $"RaftNode(Id = {Id}, Role = {CurrentRole}, Term = {CurrentTerm}, VotedFor = {VotedFor?.ToString() ?? "null"})";
    }

    public void Dispose()
    {
        _currentState?.Dispose();
    }

    public static RaftConsensusModule<TCommand, TResponse> Create(NodeId id,
                                                                  PeerGroup peerGroup,
                                                                  ILogger logger,
                                                                  ITimer electionTimer,
                                                                  ITimer heartbeatTimer,
                                                                  IBackgroundJobQueue backgroundJobQueue,
                                                                  IPersistenceManager persistenceManager,
                                                                  ICommandQueue commandQueue,
                                                                  IStateMachine<TCommand, TResponse> stateMachine,
                                                                  IStateMachineFactory<TCommand, TResponse> stateMachineFactory,
                                                                  IMetadataStorage metadataStorage,
                                                                  ISerializer<TCommand> serializer,
                                                                  IRequestQueueFactory requestQueueFactory)
    {
        var raft = new RaftConsensusModule<TCommand, TResponse>(id, peerGroup, logger, electionTimer, heartbeatTimer,
            backgroundJobQueue, persistenceManager, commandQueue, stateMachine, metadataStorage, serializer, requestQueueFactory, stateMachineFactory);
        ( ( IConsensusModule<TCommand, TResponse> ) raft ).CurrentState = raft.CreateFollowerState();
        return raft;
    }
}