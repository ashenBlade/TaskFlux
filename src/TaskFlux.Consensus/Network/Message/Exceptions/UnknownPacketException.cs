namespace TaskFlux.Consensus.Network.Message.Exceptions;

/// <summary>
/// Исключение, возникающее, когда из сети получен маркер неизвестного пакета
/// </summary>
public class UnknownPacketException : Exception
{
    /// <summary>
    /// Полученный маркер
    /// </summary>
    public byte Marker { get; }

    public override string Message => $"Получен неизвестный маркер пакета: {Marker}";

    public UnknownPacketException(byte marker)
    {
        Marker = marker;
    }
}