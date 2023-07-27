namespace TaskFlux.Network.Client.Frontend.Exceptions;

public class NotLeaderResponseException: ResponseException
{
    public NotLeaderResponseException(int leaderId)
        : base($"Коммуницирующий узел не лидер. Текущий лидер имеет ID: {leaderId}")
    { }

    public static void Throw(int leaderId)
    {
        throw new NotLeaderResponseException(leaderId);
    }
}