using Raft.Core.Peer;

namespace Raft.Core;

public interface INode
{
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

    public PeerGroup PeerGroup { get; set; }
}