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
public class RaftConsensusModule<TCommand, TResponse>
    : IConsensusModule<TCommand, TResponse>, 
      IDisposable
{
    public NodeRole CurrentRole =>
        ( ( IConsensusModule<TCommand, TResponse> ) this ).CurrentState.Role;
    public ILogger Logger { get; }
    public NodeId Id { get; }
    public Term CurrentTerm => MetadataStorage.ReadTerm();
    public NodeId? VotedFor => MetadataStorage.ReadVotedFor();
    public PeerGroup PeerGroup { get; }
    public IStateMachine<TCommand, TResponse> StateMachine { get; }
    public IMetadataStorage MetadataStorage { get; }
    public ISerializer<TCommand> Serializer { get; }

    // Выставляем вручную в .Create
    internal IConsensusModuleState<TCommand, TResponse>? CurrentState;

    IConsensusModuleState<TCommand, TResponse> IConsensusModule<TCommand, TResponse>.CurrentState
    {
        get => GetCurrentStateCheck();
        set
        {
            CurrentState?.Dispose();
            CurrentState = value;
        }
    }

    private IConsensusModuleState<TCommand, TResponse> GetCurrentStateCheck()
    {
        return CurrentState ?? throw new ArgumentNullException(nameof(CurrentState), "Текущее состояние еще не проставлено");
    }

    public ITimer ElectionTimer { get; }
    public ITimer HeartbeatTimer { get; }
    public IJobQueue JobQueue { get; }
    public ICommandQueue CommandQueue { get; } 
    public ILog Log { get; }

    internal RaftConsensusModule(
        NodeId id,
        PeerGroup peerGroup,
        ILogger logger,
        ITimer electionTimer,
        ITimer heartbeatTimer,
        IJobQueue jobQueue,
        ILog log,
        ICommandQueue commandQueue,
        IStateMachine<TCommand, TResponse> stateMachine,
        IMetadataStorage metadataStorage,
        ISerializer<TCommand> serializer)
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
        Serializer = serializer;
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

    public SubmitResponse<TResponse> Handle(SubmitRequest<TCommand> request)
    {
        return GetCurrentStateCheck().Apply(request);
    }

    public override string ToString()
    {
        return $"RaftNode(Id = {Id}, Role = {CurrentRole}, Term = {CurrentTerm}, VotedFor = {VotedFor?.ToString() ?? "null"})";
    }

    public void Dispose()
    {
        CurrentState?.Dispose();
    }
}

public static class RaftConsensusModule
{
    public static RaftConsensusModule<TCommand, TResponse> Create<TCommand, TResponse>(NodeId id,
                                                                                       PeerGroup peerGroup,
                                                                                       ILogger logger,
                                                                                       ITimer electionTimer,
                                                                                       ITimer heartbeatTimer,
                                                                                       IJobQueue jobQueue,
                                                                                       ILog log,
                                                                                       ICommandQueue commandQueue,
                                                                                       IStateMachine<TCommand, TResponse> stateMachine,
                                                                                       IMetadataStorage metadataStorage,
                                                                                       ISerializer<TCommand> serializer)
    {
        var raft = new RaftConsensusModule<TCommand, TResponse>(id, peerGroup, logger, electionTimer, heartbeatTimer, jobQueue, log, commandQueue, stateMachine, metadataStorage, serializer);
        raft.CurrentState = FollowerState.Create(raft);
        return raft;
    }
}