namespace TaskFlux.Consensus;

public interface ISnapshot
{
    // TODO: добавить сюда IncludedEntry
    /// <summary>
    /// Получить все чанки из снапшота
    /// </summary>
    /// <returns>Перечисление чанков снапшота</returns>
    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default);
}