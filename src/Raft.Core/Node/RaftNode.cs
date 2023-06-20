using System.Diagnostics;
using Raft.CommandQueue;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
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
    public Term CurrentTerm { get; set; }
    public NodeId? VotedFor { get; set; }
    public PeerGroup PeerGroup { get; }

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

    private RaftNode(NodeId id, PeerGroup peerGroup, NodeId? votedFor, Term currentTerm, ILogger logger, ITimer electionTimer, ITimer heartbeatTimer, IJobQueue jobQueue, ILog log, ICommandQueue commandQueue)
    {
        Id = id;
        Logger = logger;
        PeerGroup = peerGroup;
        ElectionTimer = electionTimer;
        HeartbeatTimer = heartbeatTimer;
        JobQueue = jobQueue;
        Log = log;
        CommandQueue = commandQueue;
        VotedFor = votedFor;
        CurrentTerm = currentTerm;
    }

    public RequestVoteResponse Handle(RequestVoteRequest request)
    {
        return CommandQueue.Enqueue(new RequestVoteCommand(request, this));
    }
    
    public AppendEntriesResponse Handle(AppendEntriesRequest request)
    {
        return CommandQueue.Enqueue(new AppendEntriesCommand(request, this));
    }
    
    public static RaftNode Create(NodeId id,
                                  PeerGroup peerGroup,
                                  NodeId? votedFor,
                                  Term currentTerm,
                                  ILogger logger,
                                  ITimer electionTimer,
                                  ITimer heartbeatTimer,
                                  IJobQueue jobQueue,
                                  ILog log,
                                  ICommandQueue commandQueue)
    {
        var raft = new RaftNode(id, peerGroup, votedFor, currentTerm, logger, electionTimer, heartbeatTimer, jobQueue, log, commandQueue);
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