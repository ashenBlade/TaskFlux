using TaskFlux.Network.Requests;

namespace TaskFlux.Network.Client.Frontend.Exceptions;

public class UnexpectedPacketResponseException: ResponseException
{
    public PacketType ActualType { get; }
    public PacketType ExpectedType { get; }

    public UnexpectedPacketResponseException(PacketType actualType, PacketType expectedType): base($"От узла пришел неожиданный тип пакета {actualType}. Ожидался {expectedType}")
    {
        ActualType = actualType;
        ExpectedType = expectedType;
    }

    public static void Throw(PacketType actualType, PacketType expectedType)
    {
        throw new UnexpectedPacketResponseException(actualType, expectedType);
    }
}