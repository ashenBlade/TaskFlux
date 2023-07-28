namespace TaskFlux.Network.Client.Frontend.Exceptions;

/// <summary>
/// Базовый класс для исключений при работе с TaskFlux
/// </summary>
public class TaskFluxException: ApplicationException
{
    public TaskFluxException()
    {
    }

    public TaskFluxException(string? message) : base(message)
    {
    }

    public TaskFluxException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}