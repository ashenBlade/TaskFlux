namespace TaskFlux.Transport.Tcp.Client.Exceptions;

public class AuthorizationException : Exception
{
    /// <summary>
    /// Сообщение об ошибке авторизации, отправленное сервером.
    /// Возможно отсутствует
    /// </summary>
    public string? ErrorMessage { get; }

    public AuthorizationException(string? errorMessage)
    {
        ErrorMessage = errorMessage;
    }
}