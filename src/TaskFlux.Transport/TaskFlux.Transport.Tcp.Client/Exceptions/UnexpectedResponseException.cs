using TaskFlux.Network.Responses;

namespace TaskFlux.Transport.Tcp.Client.Exceptions;

public class UnexpectedResponseException : Exception
{
    public NetworkResponseType Expected { get; }
    public NetworkResponseType Actual { get; }

    public UnexpectedResponseException(NetworkResponseType expected, NetworkResponseType actual)
    {
        Expected = expected;
        Actual = actual;
    }
}