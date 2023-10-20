namespace TaskFlux.Network.Client.Exceptions;

/// <summary>
/// Исключение, возникающее, когда сервер посылает Error пакет
/// </summary>
public class ServerErrorException : TaskFluxException
{
    public ServerErrorException(string serverErrorMessage)
    {
        ServerErrorMessage = serverErrorMessage;
    }

    public string ServerErrorMessage { get; }

    public override string Message => $"На сервере возникла ошибка во время обработки запроса: {ServerErrorMessage}";
}