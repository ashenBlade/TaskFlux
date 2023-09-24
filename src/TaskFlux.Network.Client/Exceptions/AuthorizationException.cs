namespace TaskFlux.Network.Client.Exceptions;

/// <summary>
/// Исключение, возникающее при ошибке авторизации на узле
/// </summary>
public class AuthorizationException : TaskFluxException
{
    public AuthorizationException(string? reason)
    {
        Reason = reason;
    }

    public string? Reason { get; }

    public override string Message => Reason is null
                                          ? $"Ошибка авторизации на узле"
                                          : $"Ошибка авторизации на узле: {Reason}";
}