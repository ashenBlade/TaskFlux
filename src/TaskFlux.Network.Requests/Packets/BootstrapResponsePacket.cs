namespace TaskFlux.Network.Requests.Packets;

/// <summary>
/// Ответ сервера на запрос установления общих настроек между клиентом и сервером
/// <br/>
/// Формат:
/// <br/>
/// | Marker(Byte) | Success(Bool) | Reason(String) |
/// </summary>
public class BootstrapResponsePacket: Packet
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
    
    public override void Accept(IPacketVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}