namespace TaskFlux.Consensus.Cluster.Network.Packets;

/// <summary>
/// Пакет, посылаемый в случае если принимаемая сторона запрашивает переотправку пакета.
/// Отправляется в случае, если принятый пакет был нарушен 
/// </summary>
public class RetransmitRequestPacket : NodePacket
{
    public override NodePacketType PacketType => NodePacketType.RetransmitRequest;

    protected override int EstimatePacketSize()
    {
        return sizeof(byte);
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        buffer[0] = ( byte ) NodePacketType.RetransmitRequest;
    }
}