namespace TaskFlux.Consensus.Network.Message.Packets;

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

    public new static RetransmitRequestPacket Deserialize(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[sizeof(uint)];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    public new static async Task<RetransmitRequestPacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        using var buffer = Rent(sizeof(uint));
        await stream.ReadExactlyAsync(buffer.GetMemory(), token);
        return DeserializePayload(buffer.GetSpan());
    }

    private static RetransmitRequestPacket DeserializePayload(Span<byte> payload)
    {
        VerifyCheckSum(payload);
        return new RetransmitRequestPacket();
    }
}