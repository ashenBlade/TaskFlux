using TaskFlux.Consensus;

namespace TaskFlux.Persistence.Tests;

public class InMemorySnapshot : ISnapshot
{
    private readonly byte[][] _data;

    public InMemorySnapshot(byte[][] data)
    {
        _data = data;
    }

    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
    {
        return _data.Select(x => new ReadOnlyMemory<byte>(x));
    }
}