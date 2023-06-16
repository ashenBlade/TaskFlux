using Raft.CommandQueue;
using Raft.Core.Commands;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.StateMachine;

public interface IStateMachine
{
    INodeState CurrentState { get; set; }
    ILogger Logger { get; }
    ITimer ElectionTimer { get; }
    ITimer HeartbeatTimer { get; }
    IJobQueue JobQueue { get; }
    ICommandQueue CommandQueue { get; }
    ILog Log { get; }
    /// <summary>
    /// ID текущего узла
    /// </summary>
    public PeerId Id { get; }

    /// <summary>
    /// Номер текущего терма
    /// </summary>
    public Term CurrentTerm { get; set; }
    /// <summary>
    /// Id кандидата, за которого проголосовала текущая нода
    /// </summary>
    public PeerId? VotedFor { get; set; }

    public PeerGroup PeerGroup { get; set; }
}