using TaskFlux.Consensus;

namespace TaskFlux.Persistence.Tests;

public class StubSnapshot(byte[] data) : ISnapshot
{
    private byte[] Data { get; } = data;

    public StubSnapshot(IEnumerable<byte> data) : this(data.ToArray())
    {
    }

    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
    {
        yield return Data;
    }
}