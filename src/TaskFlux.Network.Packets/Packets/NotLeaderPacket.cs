namespace TaskFlux.Network.Packets.Packets;

public class NotLeaderPacket : Packet
{
    public int? LeaderId { get; }

    public NotLeaderPacket(int? leaderId)
    {
        LeaderId = leaderId;
    }

    public override PacketType Type => PacketType.NotLeader;

    public override void Accept(IPacketVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}