using TaskFlux.Network.Requests.Packets;

namespace TaskFlux.Network.Requests;

public interface IAsyncPacketVisitor
{
    public ValueTask VisitAsync(CommandRequestPacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(CommandResponsePacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(ErrorResponsePacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(NotLeaderPacket packet, CancellationToken token = default);
}