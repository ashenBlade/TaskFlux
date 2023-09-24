namespace TaskFlux.Network.Client.Exceptions;

/// <summary>
/// Исключение, возникающее при ошибке подключения
/// </summary>
public class ConnectionException : TaskFluxException
{
    public ConnectionException()
    {
    }

    public ConnectionException(string? message) : base(message)
    {
    }

    public ConnectionException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}