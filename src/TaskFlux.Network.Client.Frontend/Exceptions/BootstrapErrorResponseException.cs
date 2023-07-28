namespace TaskFlux.Network.Client.Frontend.Exceptions;

public class BootstrapErrorResponseException: ResponseException
{
    public string Reason { get; }

    public BootstrapErrorResponseException(string reason): base($"Возникла ошибка во время настроки клиента и сервера. Причина: \"{reason}\"")
    {
        Reason = reason;
    }

    public static void Throw(string reason) => throw new BootstrapErrorResponseException(reason);
}