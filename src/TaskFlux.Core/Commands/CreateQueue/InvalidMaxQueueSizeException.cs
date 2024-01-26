namespace TaskFlux.Core.Commands.CreateQueue;

/// <summary>
/// Исключение, возникающее, когда передан неправильный максимальный размер очереди 
/// </summary>
public class InvalidMaxQueueSizeException : Exception
{
    public int QueueMax { get; }

    public InvalidMaxQueueSizeException(int queueMax)
    {
        QueueMax = queueMax;
    }
}