namespace TaskFlux.Consensus;

public class PeerGroup
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
    /// <remarks>Передаваемые голоса - без учета этого узла, т.е. только голоса других узлов</remarks>
    public bool IsQuorumReached(int votes)
    {
        /*
         * Кол-во узлов (других) - Мин кол-во голосов
         *
         * 1 - 1
         * 2 - 1
         * 3 - 2
         * 4 - 2
         * 5 - 3
         * 6 - 3
         * 7 - 4
         * 8 - 4
         */
        return Peers.Count <= votes * 2;
    }
}