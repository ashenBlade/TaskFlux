using Consensus.Core.Log;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Persistence.Tests;

public class StubSnapshot : ISnapshot
{
    private readonly byte[] _data;

    public StubSnapshot(byte[] data)
    {
        _data = data;
    }

    public StubSnapshot(IEnumerable<byte> data) => _data = data.ToArray();

    public void WriteTo(Stream stream, CancellationToken token = default)
    {
        var writer = new StreamBinaryWriter(stream);
        writer.WriteBuffer(_data);
    }
}