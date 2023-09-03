using Consensus.Raft.Persistence;

namespace Consensus.Raft.Tests;

public class StubSnapshot : ISnapshot
{
    private readonly byte[] _data;

    public StubSnapshot(byte[] data)
    {
        _data = data;
    }

    public StubSnapshot(IEnumerable<byte> data) => _data = data.ToArray();

    public IEnumerable<Memory<byte>> GetAllChunks(CancellationToken token = default)
    {
        yield return _data;
    }
}