using System.Buffers;
using System.Text;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Packets;

/// <summary>
/// Ответ сервера на запрос установления общих настроек между клиентом и сервером
/// <br/>
/// Формат:
/// <br/>
/// | Marker(Byte) | Success(Bool) | Reason(String) |
/// </summary>
public class BootstrapResponsePacket : Packet
{
    public override PacketType Type => PacketType.BootstrapResponse;
    public bool Success { get; }
    public string? Reason { get; }

    private BootstrapResponsePacket(bool success, string? reason)
    {
        Success = success;
        Reason = reason;
    }

    public static readonly BootstrapResponsePacket Ok = new(true, null);
    public static BootstrapResponsePacket Error(string message) => new(false, Helpers.CheckReturn(message));

    public bool TryGetError(out string message)
    {
        if (Success)
        {
            message = "";
            return false;
        }

        message = Reason!;
        return true;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        if (TryGetError(out var message))
        {
            var length = sizeof(PacketType)
                       + sizeof(byte)
                       + sizeof(int)
                       + Encoding.UTF8.GetByteCount(message);
            var buffer = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                var memory = buffer.AsMemory(0, length);
                var writer = new MemoryBinaryWriter(memory);
                writer.Write(( byte ) PacketType.BootstrapResponse);
                writer.Write(false);
                writer.Write(message);
                await stream.WriteAsync(memory, token);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        else
        {
            const int length = sizeof(PacketType)
                             + sizeof(byte);
            var buffer = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                var memory = buffer.AsMemory(0, length);
                var writer = new MemoryBinaryWriter(memory);
                writer.Write(( byte ) PacketType.BootstrapResponse);
                writer.Write(true);
                await stream.WriteAsync(memory, token);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    public new static async ValueTask<BootstrapResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
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