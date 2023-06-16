using System.Diagnostics;
using Raft.CommandQueue;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.StateMachine;

[DebuggerDisplay("Role: {CurrentState.Role}; Term: {CurrentTerm}")]
public class RaftStateMachine: IDisposable, IStateMachine
{
    public NodeRole CurrentRole => CurrentState.Role;
    public ILogger Logger { get; }
    public PeerId Id { get; }
    public Term CurrentTerm { get; set; }
    public PeerId? VotedFor { get; set; }
    public PeerGroup PeerGroup { get; set; }

    // Выставляем вручную 
    private INodeState? _currentState;
    public INodeState CurrentState
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

    private RaftStateMachine(PeerId id, PeerGroup peerGroup, PeerId? votedFor, Term currentTerm, ILogger logger, ITimer electionTimer, ITimer heartbeatTimer, IJobQueue jobQueue, ILog log, ICommandQueue commandQueue)
    {
        Id = id;
        Logger = logger;
        ElectionTimer = electionTimer;
        HeartbeatTimer = heartbeatTimer;
        JobQueue = jobQueue;
        Log = log;
        CommandQueue = commandQueue;
        PeerGroup = peerGroup;
        VotedFor = votedFor;
        CurrentTerm = currentTerm;
    }

    public RequestVoteResponse Handle(RequestVoteRequest request)
    {
        return CurrentState.Apply(request);
    }

    public HeartbeatResponse Handle(HeartbeatRequest request)
    {
        return CurrentState.Apply(request);
    }
    
    public void Dispose()
    {
        _currentState?.Dispose();
    }

    public static RaftStateMachine Create(PeerId id, PeerGroup peerGroup, PeerId? votedFor, Term currentTerm,
                                          ILogger logger,
                                          ITimer electionTimer,
                                          ITimer heartbeatTimer,
                                          IJobQueue jobQueue,
                                          ILog log,
                                          ICommandQueue commandQueue)
    {
        var raft = new RaftStateMachine(id, peerGroup, votedFor, currentTerm, logger, electionTimer, heartbeatTimer, jobQueue, log, commandQueue);
        raft._currentState = FollowerState.Create(raft);
        return raft;
    }
}