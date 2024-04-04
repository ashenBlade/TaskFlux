using System.Diagnostics;

namespace TaskFlux.Core.Queue;

/// <summary>
/// Структура для хранения пары: ID записи - Сообщение в приоритетной очереди
/// </summary>
public readonly record struct PriorityQueueData
{
    /// <summary>ID записи</summary>
    public RecordId Id { get; } = RecordId.Start;

    /// <summary>Сообщение пользователя</summary>
    public byte[] Payload { get; } = Array.Empty<byte>();

    public PriorityQueueData(RecordId id, byte[] payload)
    {
        Debug.Assert(payload is not null, "payload is not null");

        Id = id;
        Payload = payload;
    }

    public bool Equals(PriorityQueueData? other)
    {
        return other is {Id: var id} && id == Id;
    }

    public override int GetHashCode()
    {
        return Id.GetHashCode();
    }

    public void Deconstruct(out RecordId id, out byte[] payload)
    {
        id = Id;
        payload = Payload;
    }
}