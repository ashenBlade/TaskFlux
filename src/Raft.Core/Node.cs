using System.Reflection.Emit;

namespace Raft.Core;

public class Node
{
    #region Persistent

    /// <summary>
    /// Номер текущего терма
    /// </summary>
    public int CurrentTerm { get; private set; } = 1;
    
    /// <summary>
    /// Текущее состояние реплики
    /// </summary>
    public NodeRole CurrentRole { get; private set; } = NodeRole.Follower;
    
    /// <summary>
    /// Id кандидата, за которого проголосовала текущая нода
    /// </summary>
    public PeerId? VotedFor { get; private set; } = null;
    
    #endregion
    
    #region Volatile

    public int CommitIndex { get; private set; }
    public int LastApplied { get; set; }

    #endregion
    
    
}