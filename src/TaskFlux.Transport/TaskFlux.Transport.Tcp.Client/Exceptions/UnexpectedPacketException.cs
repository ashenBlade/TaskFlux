using TaskFlux.Network;

namespace TaskFlux.Transport.Tcp.Client.Exceptions;

public class UnexpectedPacketException : Exception
{
    public PacketType Expected { get; }
    public PacketType Actual { get; }

    public UnexpectedPacketException(PacketType expected, PacketType actual)
    {
        Expected = expected;
        Actual = actual;
    }
}