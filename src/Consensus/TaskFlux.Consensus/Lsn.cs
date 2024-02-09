namespace TaskFlux.Consensus.Persistence;

/// <summary>
/// Индекс записи лога команд.
/// Log Sequence Number.
/// </summary>
public readonly struct Lsn : IEquatable<Lsn>
{
    public const long TombIndex = -1;
    public static Lsn Tomb => new(TombIndex);

    /// <summary>
    /// Индекс записи лога команд
    /// </summary>
    /// <param name="value">Числовое значение индекса</param>
    public Lsn(long value)
    {
        if (value < TombIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, "LSN не может быть меньше -1");
        }

        Value = value;
    }

    /// <summary>
    /// Является ли этот индекс пустым (Tomb)
    /// </summary>
    public bool IsTomb => Value == TombIndex;

    /// <summary>
    /// Числовое значение индекса
    /// </summary>
    public long Value { get; }

    public override string ToString()
    {
        if (IsTomb)
        {
            return "LSN(Tomb)";
        }

        return $"LSN({Value})";
    }

    public static implicit operator long(Lsn lsn) => lsn.Value;
    public static implicit operator Lsn(long value) => new(value);

    public static Lsn operator +(Lsn lsn1, Lsn lsn2) => new(lsn1.Value + lsn2.Value);
    public static Lsn operator -(Lsn left, Lsn right) => new(left.Value - right.Value);


    public override int GetHashCode() => Value.GetHashCode();
    public static bool operator ==(Lsn left, Lsn right) => left.Value == right.Value;
    public static bool operator !=(Lsn left, Lsn right) => left.Value != right.Value;
    public static bool operator <(Lsn left, Lsn right) => left.Value < right.Value;
    public static bool operator >(Lsn left, Lsn right) => left.Value > right.Value;
    public static bool operator <=(Lsn left, Lsn right) => left.Value <= right.Value;
    public static bool operator >=(Lsn left, Lsn right) => left.Value >= right.Value;
    public bool Equals(Lsn other) => Value == other.Value;
    public override bool Equals(object? obj) => obj is Lsn other && Equals(other);
}