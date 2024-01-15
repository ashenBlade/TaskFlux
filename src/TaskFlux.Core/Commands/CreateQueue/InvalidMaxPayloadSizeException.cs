namespace TaskFlux.Core.Commands.CreateQueue;

/// <summary>
/// Исключение, возникающее, когда указан некорректный максимальный размер сообщения
/// </summary>
public class InvalidMaxPayloadSizeException : Exception
{
    public int PayloadSize { get; }

    public InvalidMaxPayloadSizeException(int payloadSize)
    {
        PayloadSize = payloadSize;
    }
}