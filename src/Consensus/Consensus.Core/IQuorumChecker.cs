namespace Consensus.Core;

public interface IQuorumChecker
{
    /// <summary>
    /// Проверить достигнут ли кворум
    /// </summary>
    /// <param name="votes">Количество голосов за</param>
    /// <returns>Достигнут ли кворум</returns>
    public bool IsQuorumReached(int votes);
}