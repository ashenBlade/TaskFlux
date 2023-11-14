namespace TaskFlux.Network.Packets.Packets;

public class AcknowledgeRequestPacket : Packet
{
    public static readonly AcknowledgeRequestPacket Instance = new();
    public override PacketType Type => PacketType.AcknowledgeRequest;

    public override void Accept(IPacketVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}