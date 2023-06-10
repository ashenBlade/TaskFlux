using System.IO.Pipes;

namespace Raft.Core;

public readonly record struct Term(int Value = Term.StartTerm)
{
    public const int StartTerm = 1;
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
}