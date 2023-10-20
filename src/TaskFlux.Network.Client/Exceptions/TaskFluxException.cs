using System.Runtime.Serialization;

namespace TaskFlux.Network.Client.Exceptions;

/// <summary>
/// Базовый тип исключения, связанный с логикой работы TaskFlux
/// </summary>
public class TaskFluxException : Exception
{
    public TaskFluxException()
    {
    }

    protected TaskFluxException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }

    public TaskFluxException(string? message) : base(message)
    {
    }

    public TaskFluxException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}