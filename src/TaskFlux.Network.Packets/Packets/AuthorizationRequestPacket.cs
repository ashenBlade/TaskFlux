using System.Buffers;
using TaskFlux.Network.Packets.Authorization;
using Utils.Serialization;

namespace TaskFlux.Network.Packets.Packets;

public class AuthorizationRequestPacket : Packet
{
    public AuthorizationMethod AuthorizationMethod { get; }
    public override PacketType Type => PacketType.AuthorizationRequest;

    public AuthorizationRequestPacket(AuthorizationMethod authorizationMethod)
    {
        ArgumentNullException.ThrowIfNull(authorizationMethod);
        AuthorizationMethod = authorizationMethod;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var size = sizeof(PacketType)
                 + AuthorizationMethod.EstimatePayloadSize();
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var memory = buffer.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(PacketType.AuthorizationRequest);
            AuthorizationMethod.Serialize(ref writer);
            await stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public new static async ValueTask<Packet> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var method = await AuthorizationMethod.DeserializeAsync(stream, token);
        return new AuthorizationRequestPacket(method);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}