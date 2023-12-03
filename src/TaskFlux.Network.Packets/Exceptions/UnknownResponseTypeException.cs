using TaskFlux.Network.Packets.Responses;

namespace TaskFlux.Network.Packets.Exceptions;

/// <summary>
/// Исключение, возникающее когда прочитан неизвестный маркер <see cref="NetworkResponse"/> 
/// </summary>
public class UnknownResponseTypeException : Exception
{
    public byte ResponseType { get; }

    public UnknownResponseTypeException(byte responseType)
    {
        ResponseType = responseType;
    }
}