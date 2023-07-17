using System.Diagnostics;
using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.RequestVote;
using Consensus.Core.Commands.Submit;
using Consensus.Core.Log;
using Consensus.Core.State;
using Consensus.CommandQueue;
using Consensus.StateMachine;
using Serilog;

namespace Consensus.Core;

[DebuggerDisplay("Роль: {CurrentRole}; Терм: {CurrentTerm}; Id: {Id}")]
public class RaftConsensusModule: IDisposable, IConsensusModule
{
    public NodeRole CurrentRole =>
        ( ( IConsensusModule ) this ).CurrentState.Role;
    public ILogger Logger { get; }
    public NodeId Id { get; }
    public Term CurrentTerm => MetadataStorage.ReadTerm();
    public NodeId? VotedFor => MetadataStorage.ReadVotedFor();
    public PeerGroup PeerGroup { get; }
    public IStateMachine StateMachine { get; }
    public IMetadataStorage MetadataStorage { get; }

    // Выставляем вручную в .Create
    private IConsensusModuleState? _currentState;

    IConsensusModuleState IConsensusModule.CurrentState
    {
        get => GetCurrentStateCheck();
        set
        {
            _currentState?.Dispose();
            _currentState = value;
        }
    }

    private IConsensusModuleState GetCurrentStateCheck()
    {
        return _currentState ?? throw new ArgumentNullException(nameof(_currentState), "Текущее состояние еще не проставлено");
    }

    public ITimer ElectionTimer { get; }
    public ITimer HeartbeatTimer { get; }
    public IJobQueue JobQueue { get; }
    public ICommandQueue CommandQueue { get; } 
    public ILog Log { get; }

    private RaftConsensusModule(NodeId id, PeerGroup peerGroup, ILogger logger, ITimer electionTimer, ITimer heartbeatTimer, IJobQueue jobQueue, ILog log, ICommandQueue commandQueue, IStateMachine stateMachine, IMetadataStorage metadataStorage)
    {
        Id = id;
        Logger = logger;
        PeerGroup = peerGroup;
        ElectionTimer = electionTimer;
        HeartbeatTimer = heartbeatTimer;
        JobQueue = jobQueue;
        Log = log;
        CommandQueue = commandQueue;
        StateMachine = stateMachine;
        MetadataStorage = metadataStorage;
    }

    public void UpdateState(Term newTerm, NodeId? votedFor)
    {
        MetadataStorage.Update(newTerm, votedFor);
    }

    public RequestVoteResponse Handle(RequestVoteRequest request)
    {
        return CommandQueue.Enqueue(new RequestVoteCommand(request, this));
    }
    
    public AppendEntriesResponse Handle(AppendEntriesRequest request)
    {
        return CommandQueue.Enqueue(new AppendEntriesCommand(request, this));
    }

    public SubmitResponse Handle(SubmitRequest request)
    {
        return GetCurrentStateCheck().Apply(request);
    }
    
    public static RaftConsensusModule Create(NodeId id,
                                  PeerGroup peerGroup,
                                  ILogger logger,
                                  ITimer electionTimer,
                                  ITimer heartbeatTimer,
                                  IJobQueue jobQueue,
                                  ILog log,
                                  ICommandQueue commandQueue,
                                  IStateMachine stateMachine,
                                  IMetadataStorage metadataStorage)
    {
        var raft = new RaftConsensusModule(id, peerGroup, logger, electionTimer, heartbeatTimer, jobQueue, log, commandQueue, stateMachine, metadataStorage);
        raft._currentState = FollowerState.Create(raft);
        return raft;
    }

    public override string ToString()
    {
        return $"RaftNode(Id = {Id}, Role = {CurrentRole}, Term = {CurrentTerm}, VotedFor = {VotedFor?.ToString() ?? "null"})";
    }

    public void Dispose()
    {
        _currentState?.Dispose();
    }
}