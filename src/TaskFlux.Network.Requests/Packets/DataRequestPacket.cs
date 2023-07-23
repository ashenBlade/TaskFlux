using System.Runtime.CompilerServices;

namespace TaskFlux.Network.Requests.Packets;

public class DataRequestPacket: Packet
{
    public byte[] Payload { get; }
    public override PacketType Type => PacketType.DataRequest;

    public DataRequestPacket(byte[] payload)
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