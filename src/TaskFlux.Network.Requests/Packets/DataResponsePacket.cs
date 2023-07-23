namespace TaskFlux.Network.Requests.Packets;

public class DataResponsePacket: Packet
{
    public byte[] Payload { get; }
    public override PacketType Type => PacketType.DataResponse;

    public DataResponsePacket(byte[] payload)
    {
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