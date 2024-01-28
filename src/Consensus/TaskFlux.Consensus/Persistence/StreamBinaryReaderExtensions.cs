using TaskFlux.Core;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Persistence;

public static class StreamBinaryReaderExtensions
{
    public static Lsn ReadLsn(ref this StreamBinaryReader reader)
    {
        return new Lsn(reader.ReadInt64());
    }

    public static Term ReadTerm(ref this StreamBinaryReader reader)
    {
        return new Term(reader.ReadInt64());
    }

    public static NodeId ReadNodeId(ref this StreamBinaryReader reader)
    {
        return new NodeId(reader.ReadInt32());
    }
}