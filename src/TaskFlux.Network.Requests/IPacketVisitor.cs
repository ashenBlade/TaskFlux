using TaskFlux.Network.Requests.Packets;

namespace TaskFlux.Network.Requests;

public interface IPacketVisitor
{
    public void Visit(CommandRequestPacket packet);
    public void Visit(CommandResponsePacket packet);
    public void Visit(ErrorResponsePacket packet);
    public void Visit(NotLeaderPacket packet);

}