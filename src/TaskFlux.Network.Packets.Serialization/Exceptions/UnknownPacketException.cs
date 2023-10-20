namespace TaskFlux.Network.Packets.Serialization.Exceptions;

/// <summary>
/// Исключение, возникающее при получении неизвестного маркера пакета
/// </summary>
public class UnknownPacketException : Exception
{
    public byte ReceivedMarker { get; }

    public UnknownPacketException(byte receivedMarker)
    {
        ReceivedMarker = receivedMarker;
    }

    public override string Message => $"От узла получен неизвестный маркер пакета: {ReceivedMarker}";
}