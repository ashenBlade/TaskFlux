using TaskFlux.Network.Responses.Policies;

namespace TaskFlux.Client.Exceptions;

public class PolicyViolationException : Exception
{
    public NetworkQueuePolicy Policy { get; }

    public PolicyViolationException(NetworkQueuePolicy policy)
    {
        Policy = policy;
    }
}