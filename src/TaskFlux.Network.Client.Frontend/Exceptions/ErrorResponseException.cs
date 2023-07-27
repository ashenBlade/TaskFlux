using TaskFlux.Network.Requests.Packets;

namespace TaskFlux.Network.Client.Frontend.Exceptions;

/// <summary>
/// Исключение, возникающее когда сервер в ответе возвращает <see cref="ErrorResponsePacket"/>
/// </summary>
public class ErrorResponseException: ResponseException
{
    public ErrorResponseException(string message): base(message)
    { }

    public static void Throw(string message)
    {
        throw new ErrorResponseException(message);
    }
}