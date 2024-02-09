using TaskFlux.Core;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Persistence;

public static class StreamBinaryWriterExtensions
{
    public static void Write(ref this StreamBinaryWriter writer, Term term)
    {
        writer.Write(term.Value);
    }

    public static void Write(ref this StreamBinaryWriter writer, Lsn lsn)
    {
        writer.Write(lsn.Value);
    }

    public static void Write(ref this StreamBinaryWriter writer, NodeId nodeId)
    {
        writer.Write(nodeId.Id);
    }
}