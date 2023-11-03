using Consensus.Network;

namespace Consensus.Peer.Exceptions;

public class UnexpectedPacketException : Exception
{
    /// <summary>
    /// Полученный пакет
    /// </summary>
    public RaftPacket ReceivedPacket { get; }

    /// <summary>
    /// Ожидаемый пакет
    /// </summary>
    public RaftPacketType Expected { get; }

    public UnexpectedPacketException(RaftPacket receivedPacket, RaftPacketType expected)
    {
        ReceivedPacket = receivedPacket;
        Expected = expected;
    }

    public override string Message =>
        $"От узла получен неожиданный пакет. Ожидался {Expected}. Получен: {ReceivedPacket.PacketType}";
}