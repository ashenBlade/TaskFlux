using System.Diagnostics;

namespace TaskFlux.Core.Queue;

/// <summary>
/// Пара Приоритет - Нагрузка из записи очереди
/// </summary>
public readonly record struct QueueRecordData
{
    /// <summary>Приоритет записи</summary>
    public long Priority { get; } = 0;

    /// <summary>Полезная нагрузка</summary>
    public byte[] Payload { get; } = Array.Empty<byte>();

    public QueueRecordData(long priority, byte[] payload)
    {
        Debug.Assert(payload is not null, "payload is not null");
        Priority = priority;
        Payload = payload;
    }

    public void Deconstruct(out long priority, out byte[] payload)
    {
        priority = Priority;
        payload = Payload;
    }
}