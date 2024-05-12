namespace TaskFlux.Transport.Tcp.Client.Exceptions;

public class NotLeaderException : Exception
{
    public int? CurrentLeaderId { get; }

    public NotLeaderException(int? currentLeaderId)
    {
        CurrentLeaderId = currentLeaderId;
    }
}