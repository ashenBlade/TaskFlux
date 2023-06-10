namespace Raft.Core.Peer;

/// <summary>
/// Интерфейс, представляющий другой узел 
/// </summary>
public interface IPeer
{
    /// <summary>
    /// Идентификатор узла
    /// </summary>
    public PeerId Id { get; }

    public Task SendHeartbeat(CancellationToken token);
    public Task SendRequestVote( CancellationToken token);
    public Task SendAppendEntries(CancellationToken token);

    event RequestVoteEventHandler OnRequestVote;
    event HeartbeatEventHandler OnHeartbeat;
    event AppendEntriesEventHandler OnAppendEntries;
}