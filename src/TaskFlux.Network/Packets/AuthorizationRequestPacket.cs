using System.Buffers;
using TaskFlux.Network.Authorization;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Packets;

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
            writer.Write(( byte ) PacketType.AuthorizationRequest);
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
}