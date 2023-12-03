using TaskFlux.Network.Packets.Authorization;
using Utils.Serialization;

namespace TaskFlux.Network.Packets;

internal static class BinaryWriterExtensions
{
    public static Task WriteAsync(this StreamBinaryWriter writer, PacketType type, CancellationToken token)
    {
        return writer.WriteAsync(( byte ) type, token);
    }

    public static void Write(this ref MemoryBinaryWriter writer, PacketType type)
    {
        writer.Write(( byte ) type);
    }

    public static void Write(this ref MemoryBinaryWriter writer, AuthorizationMethodType type)
    {
        writer.Write(( byte ) type);
    }
}