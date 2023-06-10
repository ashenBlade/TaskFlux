using Raft.Core.Commands;
using Raft.Core.Peer;
using Raft.Core.StateMachine;
using Serilog;

namespace Raft.Core;

public class Node
{
    private readonly RaftStateMachine _stateMachine;
    private readonly ILogger _logger;

    public Node(ILogger logger, ITimer electionTimer, ITimer heartbeatTimer)
    {
        _logger = logger;
        _stateMachine = new RaftStateMachine(this, logger, electionTimer, heartbeatTimer);
    }
    
    #region Persistent

    /// <summary>
    /// Номер текущего терма
    /// </summary>
    public Term CurrentTerm { get; set; }
    
    /// <summary>
    /// Текущее состояние реплики
    /// </summary>
    public NodeRole CurrentRole { get; private set; } = NodeRole.Follower;
    
    /// <summary>
    /// Id кандидата, за которого проголосовала текущая нода
    /// </summary>
    public PeerId? VotedFor { get; set; }
    
    #endregion
    
    #region Volatile

    /// <summary>
    /// Индекс последней закомиченной операции
    /// </summary>
    public int CommitIndex { get; private set; }
    /// <summary>
    /// Индекс последней примененной к машине состояний операции
    /// </summary>
    public int LastApplied { get; set; }
    
    /// <summary>
    /// Последний закомиченный лог
    /// </summary>
    public LogEntryInfo LastLogEntry { get; set; }

    #endregion

    /// <summary>
    /// Другие узлы кластера.
    /// Текущий узел не включается
    /// </summary>
    public IPeer[] Peers { get; set; } = Array.Empty<IPeer>();

    public async Task Apply(HeartbeatRequest request)
    {
        await _stateMachine.Apply(request, CancellationToken.None);
    }
}