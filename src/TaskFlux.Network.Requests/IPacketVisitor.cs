using TaskFlux.Network.Requests.Packets;

namespace TaskFlux.Network.Requests;

public interface IPacketVisitor
{
    public void Visit(DataRequestPacket packet);
    public void Visit(DataResponsePacket packet);
    public void Visit(ErrorResponsePacket packet);
    public void Visit(NotLeaderPacket packet);

}