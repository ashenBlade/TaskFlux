using TaskFlux.Network.Packets.Packets;

namespace TaskFlux.Network.Packets;

public interface IPacketVisitor
{
    public void Visit(CommandRequestPacket packet);
    public void Visit(CommandResponsePacket packet);
    public void Visit(ErrorResponsePacket packet);
    public void Visit(NotLeaderPacket packet);

    public void Visit(AuthorizationRequestPacket packet);
    public void Visit(AuthorizationResponsePacket packet);

    public void Visit(BootstrapRequestPacket packet);
    public void Visit(BootstrapResponsePacket packet);
}