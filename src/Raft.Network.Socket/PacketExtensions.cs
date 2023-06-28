namespace Raft.Network.Socket;

internal static class PacketExtensions
{
    public static T As<T>(this IPacket packet) where T : IPacket
    {
        return ( T ) packet;
    }
}