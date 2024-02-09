using TaskFlux.Consensus.Persistence;

namespace TaskFlux.Persistence.Tests;

public class LogEntryInfoEqualityComparer : IEqualityComparer<LogEntryInfo>
{
    public bool Equals(LogEntryInfo x, LogEntryInfo y)
    {
        return x.Term.Equals(y.Term)
            && x.Index == y.Index;
    }

    public int GetHashCode(LogEntryInfo obj)
    {
        return HashCode.Combine(obj.Term, obj.Index);
    }
}