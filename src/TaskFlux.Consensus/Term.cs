namespace TaskFlux.Consensus;

public readonly record struct Term
{
    public const long StartTerm = 1;
    public static Term Start => new(StartTerm);
    public long Value { get; } = StartTerm;

    /// <summary>
    /// Основной конструктор для типа, представляющего терм в рафте.
    /// </summary>
    /// <param name="term">Сырое значение терма</param>
    /// <exception cref="ArgumentOutOfRangeException">Значение терма меньше начального - <see cref="StartTerm"/> (0)</exception>
    public Term(long term)
    {
        if (term < StartTerm)
        {
            throw new ArgumentOutOfRangeException(nameof(term), term,
                $"Терм не может быть меньше начального {StartTerm}");
        }

        Value = term;
    }

    public static implicit operator Term(long t) => new(t);
    public static explicit operator long(Term term) => term.Value;
    public static bool operator <(Term left, Term right) => left.Value < right.Value;
    public static bool operator >(Term left, Term right) => left.Value > right.Value;
    public static bool operator <=(Term left, Term right) => left.Value <= right.Value;
    public static bool operator >=(Term left, Term right) => left.Value >= right.Value;

    public Term Increment() => new(Value + 1);

    public override string ToString()
    {
        return $"Term({Value})";
    }
}