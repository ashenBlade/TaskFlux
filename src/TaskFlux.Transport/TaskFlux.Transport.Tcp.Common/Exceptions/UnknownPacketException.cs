namespace TaskFlux.Network.Exceptions;

/// <summary>
/// Исключение, возникающее когда прочитан неизвестный маркер <see cref="PacketType"/>
/// </summary>
public class UnknownPacketException : Exception
{
    /// <summary>
    /// Неизвестный прочитанный маркер
    /// </summary>
    public byte Marker { get; }

    public UnknownPacketException(byte marker)
    {
        Marker = marker;
    }
}