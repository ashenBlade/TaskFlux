namespace TaskFlux.Application.Cluster.Network.Packets;

/// <summary>
/// Пакет, посылаемый в случае если принимаемая сторона запрашивает переотправку пакета.
/// Отправляется в случае, если принятый пакет был нарушен 
/// </summary>
public class RetransmitRequestPacket : NodePacket
{
    public override NodePacketType PacketType => NodePacketType.RetransmitRequest;

    protected override int EstimatePayloadSize()
    {
        return 0;
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
    }
}