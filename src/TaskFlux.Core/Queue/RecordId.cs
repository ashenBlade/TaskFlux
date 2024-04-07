namespace TaskFlux.Core.Queue;

/// <summary>
/// Идентификатор записи очереди
/// </summary>
/// <param name="Id">Значение ID</param>
public readonly record struct RecordId(ulong Id)
{
    /// <summary>
    /// Получить следующий ID записи
    /// </summary>
    public RecordId Increment() => new(Id + 1);

    public override string ToString()
    {
        return $"RecordId({Id})";
    }

    public static RecordId Start => new(0);

    public static bool operator <(RecordId left, RecordId right) => left.Id < right.Id;
    public static bool operator >(RecordId left, RecordId right) => left.Id > right.Id;
    public static bool operator <=(RecordId left, RecordId right) => left.Id <= right.Id;
    public static bool operator >=(RecordId left, RecordId right) => left.Id >= right.Id;

    public static implicit operator ulong(RecordId id) => id.Id;
    public static explicit operator RecordId(ulong value) => new(value);
};