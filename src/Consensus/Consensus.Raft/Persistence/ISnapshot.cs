namespace Consensus.Raft.Persistence;

public interface ISnapshot
{
    /// <summary>
    /// Получить все чанки из снапшота
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns>Поток чанков данных файла снапшота</returns>
    /// <remarks>
    /// Полученные объекты следует использовать только в области одной итерации,
    /// т.к. может использоваться пулинг. 
    /// </remarks>
    public IEnumerable<Memory<byte>> GetAllChunks(CancellationToken token = default);
}