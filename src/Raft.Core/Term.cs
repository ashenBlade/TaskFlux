namespace Raft.Core;

public readonly record struct Term
{
    public const int StartTerm = 1;
    public static Term Start => new(StartTerm);
    public int Value { get; } = StartTerm;
    public Term(int term)
    {
        if (term <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(term), term, "Терм может быть только положительным");
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