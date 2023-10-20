namespace TaskFlux.Network.Client.Exceptions;

/// <summary>
/// Исключение, возникающее когда сервер прислал неизвестный пакет
/// </summary>
public class InvalidResponseException : TaskFluxException
{
    public InvalidResponseException()
    {
    }

    public InvalidResponseException(string? message) : base(message)
    {
    }

    public InvalidResponseException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}