namespace TaskFlux.Network.Client.Frontend.Exceptions;

public class ResponseException: TaskFluxException
{
    public ResponseException()
    {
    }

    public ResponseException(string? message) : base(message)
    {
    }

    public ResponseException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}