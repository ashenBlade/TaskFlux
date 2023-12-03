using Utils.Serialization;

namespace TaskFlux.Network.Packets.Commands;

internal static class MemoryBinaryWriterExtensions
{
    public static void Write(this ref MemoryBinaryWriter writer, NetworkCommandType type)
    {
        writer.Write(( byte ) type);
    }
}