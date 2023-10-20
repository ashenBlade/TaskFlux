namespace TaskFlux.Network.Packets.Packets;

public class ClusterMetadataRequestPacket : Packet
{
    public override PacketType Type => PacketType.ClusterMetadataRequest;

    public override void Accept(IPacketVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}