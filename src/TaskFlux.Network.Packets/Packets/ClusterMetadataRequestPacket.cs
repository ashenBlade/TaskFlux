using System.Buffers;

namespace TaskFlux.Network.Packets.Packets;

public class ClusterMetadataRequestPacket : Packet
{
    public static readonly ClusterMetadataRequestPacket Instance = new();
    public override PacketType Type => PacketType.ClusterMetadataRequest;

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(PacketType));
        try
        {
            buffer[0] = ( byte ) PacketType.ClusterMetadataRequest;
            await stream.WriteAsync(buffer.AsMemory(0, sizeof(PacketType)), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}