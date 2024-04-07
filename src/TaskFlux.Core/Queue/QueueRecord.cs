using System.Diagnostics;

namespace TaskFlux.Core.Queue;

public struct QueueRecord : IEquatable<QueueRecord>
{
    /// <summary>
    /// Дефолтный конструктор использоваться не должен.
    /// Сделан только для безопасности.
    /// </summary>
    public QueueRecord()
    {
        Id = RecordId.Start;
        Priority = 0;
        Payload = Array.Empty<byte>();
    }

    public QueueRecord(RecordId id, long priority, byte[] payload)
    {
        Debug.Assert(payload is not null, "payload is not null", "Нагрузки записи не должна быть null");

        Id = id;
        Priority = priority;
        Payload = payload;
    }

    /// <summary>
    /// ID записи
    /// </summary>
    public RecordId Id { get; }

    /// <summary>
    /// Приоритет записи
    /// </summary>
    public long Priority { get; }

    /// <summary>
    /// Данные отправленные пользователем
    /// </summary>
    public byte[] Payload { get; }

    /// <summary>
    /// Получить <see cref="PriorityQueueData"/> из ID и нагрузки этой записи
    /// </summary>
    /// <returns>Данные этой записи</returns>
    public PriorityQueueData GetPriorityQueueData() => new(Id, Payload);

    public QueueRecordData GetData() => new QueueRecordData(Priority, Payload);

    public override string ToString()
    {
        return $"QueueRecord(Id = {Id.Id})";
    }

    public override int GetHashCode()
    {
        return Id.GetHashCode();
    }

    public bool Equals(QueueRecord other)
    {
        return Id == other.Id;
    }

    public static bool operator ==(QueueRecord left, QueueRecord right) => left.Equals(right);
    public static bool operator !=(QueueRecord left, QueueRecord right) => !left.Equals(right);

    public void Deconstruct(out RecordId id, out long priority, out byte[] payload)
    {
        id = Id;
        priority = Priority;
        payload = Payload;
    }

    public override bool Equals(object? obj)
    {
        return obj is QueueRecord other && Equals(other);
    }
}