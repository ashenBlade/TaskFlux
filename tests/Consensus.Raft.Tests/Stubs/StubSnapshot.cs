using Consensus.Raft.Persistence;

namespace Consensus.Raft.Tests.Stubs;

public class StubSnapshot : ISnapshot
{
    private readonly byte[] _data;

    public StubSnapshot(byte[] data)
    {
        _data = data;
    }

    public StubSnapshot(IEnumerable<byte> data) => _data = data.ToArray();

    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
    {
        yield return _data;
    }
}