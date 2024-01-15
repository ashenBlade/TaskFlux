using TaskFlux.Core;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Cluster.Network.Packets;

public class ConnectRequestPacket : NodePacket
{
    public override NodePacketType PacketType => NodePacketType.ConnectRequest;

    protected override int EstimatePacketSize()
    {
        return 1  // Маркер
             + 4; // NodeId
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) NodePacketType.ConnectRequest);
        writer.Write(Id.Id);
    }

    public NodeId Id { get; }

    public ConnectRequestPacket(NodeId id)
    {
        Id = id;
    }

    public new static ConnectRequestPacket Deserialize(Stream stream)
    {
        const int packetSize = sizeof(int); // Node Id
        Span<byte> buffer = stackalloc byte[packetSize];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    public new static async Task<ConnectRequestPacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        const int packetSize = sizeof(int); // Node Id
        using var buffer = Rent(packetSize);
        var memory = buffer.GetMemory();
        await stream.ReadExactlyAsync(memory, token);
        return DeserializePayload(memory.Span);
    }

    private static ConnectRequestPacket DeserializePayload(Span<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer);
        var id = reader.ReadInt32();
        return new ConnectRequestPacket(new NodeId(id));
    }
}