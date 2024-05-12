using System.Buffers;

namespace TaskFlux.Network.Packets;

public class NegativeAcknowledgeRequestPacket : Packet
{
    public static readonly NegativeAcknowledgeRequestPacket Instance = new();
    public override PacketType Type => PacketType.NegativeAcknowledgementRequest;

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        const int size = sizeof(PacketType);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            buffer[0] = (byte)PacketType.NegativeAcknowledgementRequest;
            await stream.WriteAsync(buffer.AsMemory(0, size), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}