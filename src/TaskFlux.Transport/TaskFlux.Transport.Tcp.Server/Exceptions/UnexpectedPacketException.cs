using TaskFlux.Network;

namespace TaskFlux.Transport.Tcp.Exceptions;

public class UnexpectedPacketException : Exception
{
    public Packet Received { get; }
    public PacketType Expected { get; }

    public UnexpectedPacketException(Packet received, PacketType expected)
    {
        Received = received;
        Expected = expected;
    }
}