namespace TaskFlux.Network.Packets.Packets;

public class CommandRequestPacket : Packet
{
    public byte[] Payload { get; }
    public override PacketType Type => PacketType.CommandRequest;

    public CommandRequestPacket(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        Payload = payload;
    }

    public override void Accept(IPacketVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}