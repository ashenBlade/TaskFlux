using TaskFlux.Consensus;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Core;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Cluster;

public static class SpanBinaryReaderExtensions
{
    public static Lsn ReadLsn(ref this SpanBinaryReader reader)
    {
        return reader.ReadInt64();
    }

    public static Term ReadTerm(ref this SpanBinaryReader reader)
    {
        return new Term(reader.ReadInt64());
    }

    public static NodeId ReadNodeId(ref this SpanBinaryReader reader)
    {
        return new NodeId(reader.ReadInt32());
    }
}