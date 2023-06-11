using Raft.Core.Peer;

namespace Raft.Core;

public class Node: INode
{
    public Node(PeerId id, PeerGroup peerGroup)
    {
        Id = id;
        PeerGroup = peerGroup;
    }

    /// <summary>
    /// ID текущего узла
    /// </summary>
    public PeerId Id { get; }
    
    #region Persistent

    /// <summary>
    /// Номер текущего терма
    /// </summary>
    public Term CurrentTerm { get; set; }

    /// <summary>
    /// Id кандидата, за которого проголосовала текущая нода
    /// </summary>
    public PeerId? VotedFor { get; set; }
    
    #endregion
    
    #region Volatile

    /// <summary>
    /// Индекс последней закомиченной операции
    /// </summary>
    public int CommitIndex { get; set; }
    /// <summary>
    /// Индекс последней примененной к машине состояний операции
    /// </summary>
    public int LastApplied { get; set; }
    
    /// <summary>
    /// Последний закомиченный лог
    /// </summary>
    public LogEntry LastLogEntry { get; set; }

    #endregion


    /// <summary>
    /// Другие узлы кластера.
    /// Текущий узел не включается
    /// </summary>
    public IReadOnlyList<IPeer> Peers => PeerGroup.Peers; 
    
    public PeerGroup PeerGroup { get; set; }
}