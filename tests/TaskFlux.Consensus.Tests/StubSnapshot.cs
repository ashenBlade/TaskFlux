namespace TaskFlux.Consensus.Tests;

public class StubSnapshot : ISnapshot
{
    public byte[] Data { get; }

    public StubSnapshot(byte[] data)
    {
        Data = data;
    }

    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
    {
        yield return Data;
    }
}