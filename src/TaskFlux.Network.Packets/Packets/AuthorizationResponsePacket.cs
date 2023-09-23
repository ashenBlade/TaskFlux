namespace TaskFlux.Network.Packets.Packets;

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

    public override void Accept(IPacketVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}