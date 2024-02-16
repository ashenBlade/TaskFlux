using System.Runtime.CompilerServices;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Cluster.Network.Packets;

public class ConnectResponsePacket : NodePacket
{
    public override NodePacketType PacketType => NodePacketType.ConnectResponse;
    public bool Success { get; }

    public ConnectResponsePacket(bool success)
    {
        Success = success;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected override int EstimatePayloadSize()
    {
        return PayloadSize;
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(Success);
    }

    public const int PayloadSize = SizeOf.Bool; // Success

    public new static ConnectResponsePacket Deserialize(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[PayloadSize]; // Success
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    public new static async Task<ConnectResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        using var buffer = Rent(PayloadSize);
        await stream.ReadExactlyAsync(buffer.GetMemory(), token);
        return DeserializePayload(buffer.GetSpan());
    }

    private static ConnectResponsePacket DeserializePayload(Span<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer);
        var success = reader.ReadBoolean();
        return new ConnectResponsePacket(success);
    }
}