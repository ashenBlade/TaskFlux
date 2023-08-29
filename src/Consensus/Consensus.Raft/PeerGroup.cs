namespace Consensus.Raft;

public class PeerGroup : IQuorumChecker
{
    private readonly IPeer[] _peers;
    public IReadOnlyList<IPeer> Peers => _peers;

    public PeerGroup(IPeer[] peers)
    {
        _peers = peers;
    }

    /// <summary>
    /// Проверить достижение кворума для данного количества голосов
    /// </summary>
    /// <param name="votes">Количество голосов за</param>
    /// <returns>Достигнут ли кворум</returns>
    public bool IsQuorumReached(int votes)
    {
        return Peers.Count / 2 + Peers.Count % 2 <= votes;
    }
}