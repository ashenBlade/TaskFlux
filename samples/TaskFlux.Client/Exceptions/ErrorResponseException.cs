namespace TaskFlux.Client.Exceptions;

public class ErrorResponseException : Exception
{
    public int Code { get; }
    public string ErrorMessage { get; }

    public ErrorResponseException(int code, string errorMessage)
    {
        Code = code;
        ErrorMessage = errorMessage;
    }
}