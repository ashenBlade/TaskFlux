namespace JobQueue.Core.Exceptions;

public class InvalidQueueNameException: FormatException
{
    /// <summary>
    /// Неправильное название очереди
    /// </summary>
    public string InvalidQueueName { get; }

    public override string Message => $"Передано неправильное название очереди: {InvalidQueueName}";

    public InvalidQueueNameException(string invalidQueueName)
    {
        ArgumentNullException.ThrowIfNull(invalidQueueName);
        
        InvalidQueueName = invalidQueueName;
    }
}