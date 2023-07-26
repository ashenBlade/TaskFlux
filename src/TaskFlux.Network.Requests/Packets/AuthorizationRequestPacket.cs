using TaskFlux.Network.Requests.Authorization;

namespace TaskFlux.Network.Requests.Packets;

public class AuthorizationRequestPacket: Packet
{
    public AuthorizationMethod AuthorizationMethod { get; }
    public override PacketType Type => PacketType.AuthorizationRequest;

    public AuthorizationRequestPacket(AuthorizationMethod authorizationMethod)
    {
        ArgumentNullException.ThrowIfNull(authorizationMethod);
        AuthorizationMethod = authorizationMethod;
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