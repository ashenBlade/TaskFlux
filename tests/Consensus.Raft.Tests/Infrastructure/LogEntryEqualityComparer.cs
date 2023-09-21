using Consensus.Raft.Persistence;

namespace Consensus.Raft.Tests.Infrastructure;

public class LogEntryEqualityComparer: IEqualityComparer<LogEntry>
{
    public static readonly LogEntryEqualityComparer Instance = new();
    public bool Equals(LogEntry x, LogEntry y)
    {
        return x.Term.Equals(y.Term)
            && x.Data.SequenceEqual(y.Data);
    }

    public int GetHashCode(LogEntry obj)
    {
        return HashCode.Combine(obj.Term, obj.Data);
    }
}