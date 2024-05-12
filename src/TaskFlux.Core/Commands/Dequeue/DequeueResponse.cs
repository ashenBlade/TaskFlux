using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Domain;

namespace TaskFlux.Core.Commands.Dequeue;

/// <summary>
/// Исходная команда для чтения записи из очереди
/// </summary>
public class DequeueResponse : Response
{
    public static readonly DequeueResponse Empty = new(false, QueueName.Default, default,
        persistent: false /* Тут не важно - сохранять или нет */);

    public static DequeueResponse CreatePersistent(QueueName queueName, QueueRecord record) =>
        new(true, queueName, record, persistent: true);

    public static DequeueResponse CreateNonPersistent(QueueName queueName, QueueRecord record) =>
        new(true, queueName, record, persistent: false);

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
    public bool Persistent { get; private set; }

    /// <summary>
    /// Название очереди, из которой необходимо читать записи
    /// </summary>
    private readonly QueueName _queueName;

    /// <summary>
    /// Прочитанная запись
    /// </summary>
    private readonly QueueRecord? _record;

    private DequeueResponse(bool success, QueueName queueName, QueueRecord? record, bool persistent)
    {
        Success = success;
        Persistent = persistent;
        _queueName = queueName;
        _record = record;
    }

    public bool TryGetResult(out QueueName queueName, out QueueRecord record)
    {
        if (_record is { } r)
        {
            queueName = _queueName;
            record = r;
            return true;
        }

        record = default!;
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