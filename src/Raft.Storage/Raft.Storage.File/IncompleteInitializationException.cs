namespace Raft.Storage.File;

public class  IncompleteInitializationException: ApplicationException
{
    public IncompleteInitializationException()
    { }

    public IncompleteInitializationException(string? message) : base(message)
    {
    }

    public IncompleteInitializationException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}