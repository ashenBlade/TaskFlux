using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Dequeue;

/// <summary>
/// Исходная команда для чтения записи из очереди
/// </summary>
public class DequeueResponse : Response
{
    public static readonly DequeueResponse Empty = new(false, QueueName.Default, 0, null, false);

    public static DequeueResponse Create(QueueName queueName, long key, byte[] payload, bool produceDelta) =>
        new(true, queueName, key, payload, produceDelta);

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

    /// <summary>
    /// Отдавать ли значение при вызове <see cref="TryGetDelta"/>
    /// </summary>
    private readonly bool _produceDelta;

    private DequeueResponse(bool success, QueueName queueName, long key, byte[]? payload, bool produceDelta)
    {
        Success = success;
        _queueName = queueName;
        _key = key;
        _produceDelta = produceDelta;
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

    /// <summary>
    /// Создать <see cref="DequeueResponse"/> из текущего, который вернет дельту при вызове <see cref="TryGetDelta"/>
    /// </summary>
    public DequeueResponse WithDeltaProducing() =>
        _produceDelta
            ? this
            : new DequeueResponse(Success, _queueName, _key, _message, true);

    /// <summary>
    /// Создать <see cref="DequeueResponse"/> из текущего, который не будет возвращать дельту при вызове <see cref="TryGetDelta"/>
    /// </summary>
    public DequeueResponse WithoutDeltaProducing() =>
        _produceDelta
            ? new DequeueResponse(Success, _queueName, _key, _message, false)
            : this;
}