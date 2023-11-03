namespace TaskQueue.Core.Exceptions;

public class InvalidQueueNameException : FormatException
{
    /// <summary>
    /// Неправильное название очереди
    /// </summary>
    public string? InvalidQueueName { get; }

    public override string Message => InvalidQueueName is { } name
                                          ? $"Передано неправильное название очереди: {name}"
                                          : "Передано неправильное название очереди";

    public InvalidQueueNameException(string invalidQueueName)
    {
        ArgumentNullException.ThrowIfNull(invalidQueueName);

        InvalidQueueName = invalidQueueName;
    }

    public InvalidQueueNameException()
    {
        InvalidQueueName = null;
    }
}