namespace TaskFlux.Consensus.Persistence;

public interface IChunkEnumerator : IDisposable
{
    public IEnumerable<Memory<byte>> GetAllChunks(CancellationToken token = default);
}