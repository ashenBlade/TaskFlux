using TaskFlux.Network.Responses;

namespace TaskFlux.Network.Exceptions;

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