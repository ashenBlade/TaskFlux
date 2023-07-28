namespace TaskFlux.Network.Client.Frontend.Exceptions;

public class AuthorizationFailedException: TaskFluxException
{
    public string Reason { get; }

    public AuthorizationFailedException(string reason): base($"Авторизация на узле была неуспешной. Причина: {reason}")
    {
        Reason = reason;
    }

    public static void Throw(string reason)
    {
        throw new AuthorizationFailedException(reason);
    }
}