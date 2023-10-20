using TaskFlux.Network.Packets.Packets;

namespace TaskFlux.Network.Packets;

public interface IAsyncPacketVisitor
{
    public ValueTask VisitAsync(CommandRequestPacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(CommandResponsePacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(ErrorResponsePacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(NotLeaderPacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(AuthorizationRequestPacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(AuthorizationResponsePacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(BootstrapResponsePacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(BootstrapRequestPacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(ClusterMetadataResponsePacket packet, CancellationToken token = default);
    public ValueTask VisitAsync(ClusterMetadataRequestPacket packet, CancellationToken token = default);
}