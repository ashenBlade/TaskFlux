using System.Net;

namespace TaskFlux.Network.Packets.Packets;

public class ClusterMetadataResponsePacket : Packet
{
    /// <summary>
    /// Адреса узлов
    /// </summary>
    public IReadOnlyList<EndPoint> EndPoints { get; }

    /// <summary>
    /// Id текущего лидера кластера
    /// </summary>
    public int? LeaderId { get; }

    /// <summary>
    /// Id ответившего узла
    /// </summary>
    public int RespondingId { get; }

    public ClusterMetadataResponsePacket(IReadOnlyList<EndPoint> endPoints, int? leaderId, int respondingId)
    {
        EndPoints = endPoints;
        LeaderId = leaderId;
        RespondingId = respondingId;
    }

    public override PacketType Type => PacketType.ClusterMetadataResponse;

    public override void Accept(IPacketVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}