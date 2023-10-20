namespace TaskFlux.Network.Packets.Serialization.Exceptions;

/// <summary>
/// Класс исключения при ошибке десериализации сетевого пакета
/// </summary>
public class PacketDeserializationException : Exception
{
    public PacketType PacketType { get; }

    public PacketDeserializationException(PacketType packetType, string? message) : base(message)
    {
        PacketType = packetType;
    }

    public PacketDeserializationException(PacketType packetType, string? message, Exception? innerException) : base(
        message, innerException)
    {
        PacketType = packetType;
    }
}