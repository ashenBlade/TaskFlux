using System.Buffers;

namespace TaskFlux.Network.Packets;

public class AcknowledgeRequestPacket : Packet
{
    public static readonly AcknowledgeRequestPacket Instance = new();
    public override PacketType Type => PacketType.AcknowledgeRequest;

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        const int size = sizeof(PacketType);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            buffer[0] = ( byte ) PacketType.AcknowledgeRequest;
            await stream.WriteAsync(buffer.AsMemory(0, size), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}