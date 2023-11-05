using TaskFlux.Core;
using Utils.Serialization;

namespace Consensus.Network.Packets;

// TODO: добавить поле версии
public class ConnectRequestPacket : RaftPacket
{
    public override RaftPacketType PacketType => RaftPacketType.ConnectRequest;

    protected override int EstimatePacketSize()
    {
        return 1  // Маркер
             + 4; // NodeId
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) RaftPacketType.ConnectRequest);
        writer.Write(Id.Id);
    }

    public NodeId Id { get; }

    public ConnectRequestPacket(NodeId id)
    {
        Id = id;
    }
}