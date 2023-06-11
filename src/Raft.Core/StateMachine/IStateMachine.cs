using Raft.Core.Log;
using Serilog;

namespace Raft.Core.StateMachine;

internal interface IStateMachine
{
    NodeRole CurrentRole { get; }
    INodeState CurrentState { get; set; }
    ILogger Logger { get; }
    INode Node { get; }
    ITimer ElectionTimer { get; }
    ITimer HeartbeatTimer { get; }
    IJobQueue JobQueue { get; }
    ILog Log { get; }
}