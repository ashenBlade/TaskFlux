namespace Consensus.Network.Packets;

/// <summary>
/// Пакет, посылаемый в случае если принимаемая сторона запрашивает переотправку пакета.
/// Отправляется в случае, если принятый пакет был нарушен 
/// </summary>
public class RetransmitRequestPacket : RaftPacket
{
    public override RaftPacketType PacketType => RaftPacketType.RetransmitRequest;

    protected override int EstimatePacketSize()
    {
        return sizeof(byte);
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        buffer[0] = ( byte ) RaftPacketType.RetransmitRequest;
    }
}