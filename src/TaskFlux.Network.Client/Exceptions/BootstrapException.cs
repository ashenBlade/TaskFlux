namespace TaskFlux.Network.Client.Exceptions;

/// <summary>
/// Исключение, возникающее при ошибке во время начальной настройки клиента
/// </summary>
/// <example>
/// Версия клиента и сервера несовместимы
/// </example>
public class BootstrapException : TaskFluxException
{
    public BootstrapException(string? serverErrorMessage)
    {
        ServerErrorMessage = serverErrorMessage;
    }

    public string? ServerErrorMessage { get; }

    public override string Message => ServerErrorMessage is null
                                          ? "Ошибка во время начальной настройки клиента"
                                          : $"Ошибка во время начальной настройки клиента: {ServerErrorMessage}";
}