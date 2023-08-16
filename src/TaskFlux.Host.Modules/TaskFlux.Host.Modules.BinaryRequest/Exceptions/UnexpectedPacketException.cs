using TaskFlux.Network.Requests;

namespace TaskFlux.Host.Modules.BinaryRequest.Exceptions;

public class UnexpectedPacketException: ApplicationException
{
    public PacketType PacketType { get; }

    public UnexpectedPacketException(PacketType packetType) : this(packetType,
        $"От клиента пришел неожиданный тип пакета: {packetType}")
    { }

    public UnexpectedPacketException(PacketType packetType, string? message, Exception? innerException = null) : base(message, innerException)
    {
        PacketType = packetType;
    }
}