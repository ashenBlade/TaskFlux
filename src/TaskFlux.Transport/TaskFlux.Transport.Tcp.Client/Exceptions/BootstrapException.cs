namespace TaskFlux.Transport.Tcp.Client.Exceptions;

public class BootstrapException : Exception
{
    public string? ErrorReason { get; }

    public BootstrapException(string? errorReason)
    {
        ErrorReason = errorReason;
    }
}