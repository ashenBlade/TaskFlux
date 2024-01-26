using System.Buffers;
using System.Text;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Packets;

public class AuthorizationResponsePacket : Packet
{
    public override PacketType Type => PacketType.AuthorizationResponse;
    public bool Success { get; }
    public string? ErrorReason { get; }

    private AuthorizationResponsePacket(bool success, string? errorReason)
    {
        Success = success;
        ErrorReason = errorReason;
    }

    public static readonly AuthorizationResponsePacket Ok = new AuthorizationResponsePacket(true, null);

    public static AuthorizationResponsePacket Error(string reason) =>
        new AuthorizationResponsePacket(false, Helpers.CheckReturn(reason));

    public bool TryGetError(out string reason)
    {
        if (Success)
        {
            reason = "";
            return false;
        }

        reason = ErrorReason!;
        return true;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        if (TryGetError(out var error))
        {
            var errorSize = sizeof(PacketType)
                          + sizeof(bool)
                          + sizeof(int)
                          + Encoding.UTF8.GetByteCount(error);
            var buffer = ArrayPool<byte>.Shared.Rent(errorSize);
            try
            {
                var memory = buffer.AsMemory(0, errorSize);
                var writer = new MemoryBinaryWriter(memory);
                writer.Write(( byte ) PacketType.AuthorizationResponse);
                writer.Write(false);
                writer.Write(error);
                await stream.WriteAsync(memory, token);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        else
        {
            const int successSize = sizeof(PacketType)
                                  + sizeof(bool);
            var buffer = ArrayPool<byte>.Shared.Rent(successSize);
            try
            {
                var memory = buffer.AsMemory(0, successSize);
                var writer = new MemoryBinaryWriter(memory);
                writer.Write(( byte ) PacketType.AuthorizationResponse);
                writer.Write(true);
                await stream.WriteAsync(memory, token);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    public new static async ValueTask<AuthorizationResponsePacket> DeserializeAsync(
        Stream stream,
        CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var success = await reader.ReadBoolAsync(token);
        if (success)
        {
            return Ok;
        }

        var reason = await reader.ReadStringAsync(token);
        return Error(reason);
    }
}