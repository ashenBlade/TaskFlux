using Raft.CommandQueue;
using Raft.Core.Commands;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.StateMachine;

public interface IStateMachine
{
    NodeRole CurrentRole { get; }
    INodeState CurrentState { get; set; }
    ILogger Logger { get; }
    INode Node { get; }
    ITimer ElectionTimer { get; }
    ITimer HeartbeatTimer { get; }
    IJobQueue JobQueue { get; }
    ICommandQueue CommandQueue { get; }
    ILog Log { get; }
}