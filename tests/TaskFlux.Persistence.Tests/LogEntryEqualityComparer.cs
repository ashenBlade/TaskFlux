using TaskFlux.Consensus.Persistence;

namespace TaskFlux.Persistence.Tests;

public class LogEntryEqualityComparer : IEqualityComparer<LogEntry>
{
    public static readonly LogEntryEqualityComparer Instance = new();

    public bool Equals(LogEntry? x, LogEntry? y)
    {
        ArgumentNullException.ThrowIfNull(x);
        ArgumentNullException.ThrowIfNull(y);

        return x.Term.Equals(y.Term) && x.Data.SequenceEqual(y.Data);
    }

    public int GetHashCode(LogEntry obj)
    {
        return HashCode.Combine(obj.Term, obj.Data);
    }
}