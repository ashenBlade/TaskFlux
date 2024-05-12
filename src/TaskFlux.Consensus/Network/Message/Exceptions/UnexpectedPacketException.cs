namespace TaskFlux.Consensus.Network.Message.Exceptions;

public class UnexpectedPacketException : Exception
{
    /// <summary>
    /// Полученный пакет
    /// </summary>
    public NodePacket ReceivedPacket { get; }

    /// <summary>
    /// Ожидаемый пакет
    /// </summary>
    public NodePacketType Expected { get; }

    public UnexpectedPacketException(NodePacket receivedPacket, NodePacketType expected)
    {
        ReceivedPacket = receivedPacket;
        Expected = expected;
    }

    public override string Message =>
        $"От узла получен неожиданный пакет. Ожидался {Expected}. Получен: {ReceivedPacket.PacketType}";
}