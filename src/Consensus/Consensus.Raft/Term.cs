namespace Consensus.Raft;

public readonly record struct Term
{
    public const int StartTerm = 1;
    public static Term Start => new(StartTerm);
    public int Value { get; } = StartTerm;

    /// <summary>
    /// Основной конструктор для типа, представляющего терм в рафте.
    /// </summary>
    /// <param name="term">Сырое значение терма</param>
    /// <exception cref="ArgumentOutOfRangeException">Значение терма меньше начального - <see cref="StartTerm"/> (0)</exception>
    public Term(int term)
    {
        if (term < StartTerm)
        {
            throw new ArgumentOutOfRangeException(nameof(term), term,
                $"Терм не может быть меньше начального {StartTerm}");
        }

        Value = term;
    }

    public static explicit operator int(Term term) => term.Value;
    public static bool operator <(Term left, Term right) => left.Value < right.Value;
    public static bool operator >(Term left, Term right) => left.Value > right.Value;
    public static bool operator <=(Term left, Term right) => left.Value <= right.Value;
    public static bool operator >=(Term left, Term right) => left.Value >= right.Value;

    public bool Equals(Term? other)
    {
        return other?.Value == Value;
    }

    public Term Increment() => new(Value + 1);

    public override string ToString()
    {
        return $"Term({Value})";
    }
}