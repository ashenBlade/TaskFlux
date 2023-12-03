using System.Buffers;
using Utils.Serialization;

namespace TaskFlux.Network.Packets.Packets;

/// <summary>
/// Пакет для установления общих настроек для общения клиента и сервера
/// <br/>
/// Формат:
/// <br/>
/// | Marker(Byte) | Major(Int32) | Minor(Int32) | Patch(Int32) |
/// 
/// </summary>
public class BootstrapRequestPacket : Packet
{
    public int Major { get; }
    public int Minor { get; }
    public int Patch { get; }

    public BootstrapRequestPacket(int major, int minor, int patch)
    {
        Major = major;
        Minor = minor;
        Patch = patch;
    }

    public override PacketType Type => PacketType.BootstrapRequest;

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        const int length = sizeof(PacketType)
                         + sizeof(int)
                         + sizeof(int)
                         + sizeof(int);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var memory = buffer.AsMemory(0, length);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(( byte ) PacketType.BootstrapRequest);
            writer.Write(Major);
            writer.Write(Minor);
            writer.Write(Patch);
            await stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public new static async ValueTask<BootstrapRequestPacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var major = await reader.ReadInt32Async(token);
        var minor = await reader.ReadInt32Async(token);
        var patch = await reader.ReadInt32Async(token);
        return new BootstrapRequestPacket(major, minor, patch);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}