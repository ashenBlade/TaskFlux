using TaskFlux.Network.Requests.Packets;

namespace TaskFlux.Network.Requests;

public interface IAsyncPacketVisitor
{
    public ValueTask VisitAsync(DataRequestPacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(DataResponsePacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(ErrorResponsePacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(NotLeaderPacket packet, CancellationToken token = default);
}