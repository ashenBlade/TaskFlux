namespace TaskFlux.Commands.Serialization.Exceptions;

/// <summary>
/// Базовый класс для исключений, касающихся десерилазации
/// </summary>
public class DeserializationException : Exception
{
    public DeserializationException()
    {
    }

    public DeserializationException(string? message) : base(message)
    {
    }

    public DeserializationException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}