using TaskFlux.Application.Cluster.Network;
using TaskFlux.Consensus;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Core;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Cluster;

public static class SpanBinaryWriterExtensions
{
    public static void Write(ref this SpanBinaryWriter writer, Term term)
    {
        writer.Write(term.Value);
    }

    public static void Write(ref this SpanBinaryWriter writer, Lsn lsn)
    {
        writer.Write(lsn.Value);
    }

    public static void Write(ref this SpanBinaryWriter writer, NodePacketType packetType)
    {
        writer.Write(( byte ) packetType);
    }

    public static void Write(ref this SpanBinaryWriter writer, NodeId nodeId)
    {
        writer.Write(nodeId.Id);
    }
}