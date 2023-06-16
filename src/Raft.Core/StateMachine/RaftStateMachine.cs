using System.Diagnostics;
using Raft.CommandQueue;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.StateMachine;

[DebuggerDisplay("Role: {CurrentState.Role}; Term: {Node.CurrentTerm}")]
public class RaftStateMachine: IDisposable, IStateMachine
{
    public NodeRole CurrentRole => CurrentState.Role;
    public ILogger Logger { get; }
    public INode Node { get; }

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

    private RaftStateMachine(INode node, ILogger logger, ITimer electionTimer, ITimer heartbeatTimer, IJobQueue jobQueue, ILog log, ICommandQueue commandQueue)
    {
        Node = node;
        Logger = logger;
        ElectionTimer = electionTimer;
        HeartbeatTimer = heartbeatTimer;
        JobQueue = jobQueue;
        Log = log;
        CommandQueue = commandQueue;
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

    public static RaftStateMachine Create(INode node,
                                         ILogger logger,
                                         ITimer electionTimer,
                                         ITimer heartbeatTimer,
                                         IJobQueue jobQueue,
                                         ILog log,
                                         ICommandQueue commandQueue)
    {
        var raft = new RaftStateMachine(node, logger, electionTimer, heartbeatTimer, jobQueue, log, commandQueue);
        var state = FollowerState.Create(raft);
        raft._currentState = state;
        return raft;
    }
}