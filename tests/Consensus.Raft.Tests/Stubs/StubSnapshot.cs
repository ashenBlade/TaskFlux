using Consensus.Raft.Persistence;

namespace Consensus.Raft.Tests.Stubs;

public class StubSnapshot : ISnapshot
{
    public byte[] Data { get; }

    public StubSnapshot(byte[] data)
    {
        Data = data;
    }

    public StubSnapshot(IEnumerable<byte> data) => Data = data.ToArray();

    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
    {
        yield return Data;
    }
}