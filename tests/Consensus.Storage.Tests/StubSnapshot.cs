using Consensus.Raft.Persistence;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Storage.Tests;

public class StubSnapshot : ISnapshot
{
    private readonly byte[] _data;

    public StubSnapshot(byte[] data)
    {
        _data = data;
    }

    public StubSnapshot(IEnumerable<byte> data) => _data = data.ToArray();

    public void WriteTo(Stream destination, CancellationToken token = default)
    {
        var writer = new StreamBinaryWriter(destination);
        writer.Write(_data);
    }
}