using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Dequeue;

/// <summary>
/// Исходная команда для чтения записи из очереди
/// </summary>
public class DequeueResponse : Response
{
    public static readonly DequeueResponse Empty = new(false, QueueName.Default, 0, null);

    public static DequeueResponse Create(QueueName queueName, long key, byte[] payload) =>
        new(true, queueName, key, payload);

    public override ResponseType Type => ResponseType.Dequeue;

    public bool Success { get; }

    /// <summary>
    /// Название очереди, из которой необходимо читать записи
    /// </summary>
    private readonly QueueName _queueName;

    /// <summary>
    /// Прочитанный ключ
    /// </summary>
    private readonly long _key;

    /// <summary>
    /// Прочитанное сообщение
    /// </summary>
    private readonly byte[] _message;

    private DequeueResponse(bool success, QueueName queueName, long key, byte[]? payload)
    {
        Success = success;
        _queueName = queueName;
        _key = key;
        _message = payload ?? Array.Empty<byte>();
    }

    public bool TryGetResult(out QueueName queueName, out long key, out byte[] payload)
    {
        if (Success)
        {
            queueName = _queueName;
            key = _key;
            payload = _message;
            return true;
        }

        key = 0;
        payload = Array.Empty<byte>();
        queueName = QueueName.Default;
        return false;
    }

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}