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
        Priority = 0;
        Payload = Array.Empty<byte>();
    }

    public QueueRecord(long priority, byte[] payload)
    {
        Debug.Assert(payload is not null, "payload is not null", "Нагрузки записи не должна быть null");

        Priority = priority;
        Payload = payload;
    }

    public long Priority { get; }
    public byte[] Payload { get; }

    public override string ToString()
    {
        return "QueueRecord()";
    }

    private int? _hashCode;

    private int CalculateHashCode()
    {
        const int magic = 314159;
        var initial = ( Payload.Length + Priority ).GetHashCode();

        return Payload.Aggregate(initial, (hashCode, value) => unchecked( hashCode * magic + value ));
    }

    public override int GetHashCode()
    {
        return _hashCode ??= CalculateHashCode();
    }

    public bool Equals(QueueRecord other)
    {
        if (_hashCode is { } hc && other._hashCode is { } ohc && hc != ohc)
        {
            return false;
        }

        // TODO: неоптимально - заменить на ID 
        return Priority == other.Priority
            && Payload.SequenceEqual(other.Payload);
    }

    public void Deconstruct(out long priority, out byte[] payload)
    {
        priority = Priority;
        payload = Payload;
    }

    public override bool Equals(object? obj)
    {
        return obj is QueueRecord other && Equals(other);
    }
}