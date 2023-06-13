using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.StateMachine;

public class RaftStateMachine: IDisposable, IStateMachine
{
    public NodeRole CurrentRole => _currentState.Role;
    public ILogger Logger { get; }
    public INode Node { get; }
    
    // Выставляем вручную 
    private NodeState _currentState = null!;
    public NodeState CurrentState
    {
        get => _currentState;
        set
        {
            _currentState?.Dispose();
            _currentState = value;
        }
    }

    public ITimer ElectionTimer { get; }
    public ITimer HeartbeatTimer { get; }
    public IJobQueue JobQueue { get; }
    public ILog Log { get; }
    
    private RaftStateMachine(INode node, ILogger logger, ITimer electionTimer, ITimer heartbeatTimer, IJobQueue jobQueue, ILog log)
    {
        Node = node;
        Logger = logger;
        ElectionTimer = electionTimer;
        HeartbeatTimer = heartbeatTimer;
        JobQueue = jobQueue;
        Log = log;
    }

    public RequestVoteResponse Handle(RequestVoteRequest request, CancellationToken token = default)
    {
        return CurrentState.Apply(request, token);
    }

    public HeartbeatResponse Handle(HeartbeatRequest request, CancellationToken token = default)
    {
        while (true)
        {
            try
            {
                return CurrentState.Apply(request, token);
            }
            catch (InvalidOperationException invalidOperation)
            {
                Logger.Warning(invalidOperation, "Во время применения операции состояние машины изменилось. Делаю повторную попытку");
            }
        }
    }
    public void Dispose()
    {
        _currentState.Dispose();
    }

    public static RaftStateMachine Start(INode node,
                                         ILogger logger,
                                         ITimer electionTimer,
                                         ITimer heartbeatTimer,
                                         IJobQueue jobQueue,
                                         ILog log)
    {
        var raft = new RaftStateMachine(node, logger, electionTimer, heartbeatTimer, jobQueue, log);
        var state = FollowerState.Create(raft);
        
        raft.ElectionTimer.Start();
        raft.Node.VotedFor = null;
        
        raft._currentState = state;
        return raft;
    }
}