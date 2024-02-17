using TaskFlux.Core;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Cluster.Network.Packets;

public class ConnectRequestPacket : NodePacket
{
    public override NodePacketType PacketType => NodePacketType.ConnectRequest;
    public NodeId Id { get; }

    protected override int EstimatePayloadSize()
    {
        return SizeOf.NodeId; // ID узла
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(Id.Id);
    }


    public ConnectRequestPacket(NodeId id)
    {
        Id = id;
    }

    private const int PayloadSize = SizeOf.NodeId;

    public new static ConnectRequestPacket Deserialize(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[PayloadSize + sizeof(uint)];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    public new static async Task<ConnectRequestPacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        using var buffer = Rent(PayloadSize + sizeof(uint));
        var memory = buffer.GetMemory();
        await stream.ReadExactlyAsync(memory, token);
        return DeserializePayload(memory.Span);
    }

    private static ConnectRequestPacket DeserializePayload(Span<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer);
        var id = reader.ReadNodeId();
        return new ConnectRequestPacket(id);
    }
}