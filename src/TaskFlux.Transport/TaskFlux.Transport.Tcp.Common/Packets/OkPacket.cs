using System.Buffers;

namespace TaskFlux.Network.Packets;

public class OkPacket : Packet
{
    public static readonly OkPacket Instance = new();
    public override PacketType Type => PacketType.Ok;

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        const int size = sizeof(byte);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            buffer[0] = (byte)PacketType.Ok;
            await stream.WriteAsync(buffer.AsMemory(0, size), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}