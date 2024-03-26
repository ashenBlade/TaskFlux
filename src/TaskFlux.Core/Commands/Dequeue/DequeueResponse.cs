using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands.Dequeue;

/// <summary>
/// Исходная команда для чтения записи из очереди
/// </summary>
public class DequeueResponse : Response
{
    public static readonly DequeueResponse Empty = new(false, QueueName.Default, 0, null,
        persistent: false /* Тут не важно - сохранять или нет */);

    public static DequeueResponse CreatePersistent(QueueName queueName, long key, byte[] payload) =>
        new(true, queueName, key, payload, persistent: true);

    public static DequeueResponse CreateNonPersistent(QueueName queueName, long key, byte[] payload) =>
        new(true, queueName, key, payload, persistent: false);

    public override ResponseType Type => ResponseType.Dequeue;

    /// <summary>
    /// Прочитана ли запись
    /// </summary>
    public bool Success { get; }

    /// <summary>
    /// Следует ли результат операции сохранять сразу же.
    /// Используется для получения дельты удаления записи.
    /// Изначально <c>false</c> 
    /// </summary>
    public bool Persistent { get; private set; } = false;

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

    private DequeueResponse(bool success, QueueName queueName, long key, byte[]? payload, bool persistent)
    {
        Success = success;
        Persistent = persistent;
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

    /// <summary>
    /// Выставить флаг <see cref="Persistent"/> в <c>true</c>.
    /// Тогда результат будет сохранен сразу.
    /// </summary>
    public void MakePersistent() => Persistent = true;

    /// <summary>
    /// Выставить флаг <see cref="Persistent"/> в <c>false</c>
    /// </summary>
    public void MakeNonPersistent() => Persistent = false;

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}