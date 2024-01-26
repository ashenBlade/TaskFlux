namespace TaskFlux.Network.Exceptions;

public class UnknownQueuePolicyException : Exception
{
    /// <summary>
    /// Полученный маркер политики очереди
    /// </summary>
    public int Marker { get; }

    public UnknownQueuePolicyException(int marker)
    {
        Marker = marker;
    }
}