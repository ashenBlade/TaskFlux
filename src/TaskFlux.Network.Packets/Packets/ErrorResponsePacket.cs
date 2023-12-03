using System.Buffers;
using System.Text;
using Utils.Serialization;

namespace TaskFlux.Network.Packets.Packets;

public class ErrorResponsePacket : Packet
{
    public static readonly ErrorResponsePacket EmptyErrorMessagePacket = new(string.Empty);
    public string Message { get; }
    public override PacketType Type => PacketType.ErrorResponse;

    public ErrorResponsePacket(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        Message = message;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var estimatedSize = sizeof(PacketType)
                          + sizeof(int)
                          + Encoding.UTF8.GetByteCount(Message);
        var array = ArrayPool<byte>.Shared.Rent(estimatedSize);
        try
        {
            var buffer = array.AsMemory(0, estimatedSize);
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write(( byte ) PacketType.ErrorResponse);
            writer.Write(Message);
            await stream.WriteAsync(buffer, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(array);
        }
    }

    public new static async Task<ErrorResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var message = await reader.ReadStringAsync(token);
        return new ErrorResponsePacket(message);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}