using System.Diagnostics;
using Raft.CommandQueue;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Commands.Submit;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.Node;

[DebuggerDisplay("Роль: {CurrentRole}; Терм: {CurrentTerm}; Id: {Id}")]
public class RaftNode: IDisposable, INode
{
    public NodeRole CurrentRole =>
        ( ( INode ) this ).CurrentState.Role;
    public ILogger Logger { get; }
    public NodeId Id { get; }
    public Term CurrentTerm => MetadataStorage.ReadTerm();
    public NodeId? VotedFor => MetadataStorage.ReadVotedFor();
    public PeerGroup PeerGroup { get; }
    public IStateMachine StateMachine { get; }
    public IMetadataStorage MetadataStorage { get; }

    // Выставляем вручную в .Create
    private INodeState? _currentState;

   INodeState INode.CurrentState
    {
        get => _currentState ?? throw new ArgumentNullException(nameof(_currentState), "Текущее состояние еще не проставлено");
        set
        {
            _currentState?.Dispose();
            _currentState = value;
        }
    }

    public ITimer ElectionTimer { get; }
    public ITimer HeartbeatTimer { get; }
    public IJobQueue JobQueue { get; }
    public ICommandQueue CommandQueue { get; } 
    public ILog Log { get; }

    private RaftNode(NodeId id, PeerGroup peerGroup, ILogger logger, ITimer electionTimer, ITimer heartbeatTimer, IJobQueue jobQueue, ILog log, ICommandQueue commandQueue, IStateMachine stateMachine, IMetadataStorage metadataStorage)
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
        return CommandQueue.Enqueue(new SubmitCommand(request, this));
    }
    
    public static RaftNode Create(NodeId id,
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
        var raft = new RaftNode(id, peerGroup, logger, electionTimer, heartbeatTimer, jobQueue, log, commandQueue, stateMachine, metadataStorage);
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