namespace TaskFlux.Network.Requests.Packets;

/// <summary>
/// Пакет для установления общих настроек для общения клиента и сервера
/// <br/>
/// Формат:
/// <br/>
/// | Marker(Byte) | Major(Int32) | Minor(Int32) | Patch(Int32) |
/// 
/// </summary>
public class BootstrapRequestPacket: Packet
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
    public override void Accept(IPacketVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}