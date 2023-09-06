namespace Consensus.Raft.Persistence;

public interface ISnapshot
{
    /// <summary>
    /// Получить все чанки из снапшота
    /// </summary>
    /// <returns>Перечисление чанков снапшота</returns>
    public IEnumerable<Memory<byte>> GetAllChunks(CancellationToken token = default);
}