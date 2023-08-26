using Consensus.Raft.Persistence;

namespace Consensus.Storage.Tests;

public class LogEntryEqualityComparer: IEqualityComparer<LogEntry>
{
    public bool Equals(LogEntry x, LogEntry y)
    {
        return x.Term.Equals(y.Term)
            && PayloadEquals(x.Data, y.Data);
    }

    public int GetHashCode(LogEntry obj)
    {
        return HashCode.Combine(obj.Term, obj.Data);
    }
    
    private static bool PayloadEquals(byte[] first, byte[] second)
    {
        if (first.Length != second.Length)
        {
            return false;
        }

        for (int i = 0; i < first.Length; i++)
        {
            if (first[i] != second[i])
            {
                return false;
            }
        }

        return true;
    }
}