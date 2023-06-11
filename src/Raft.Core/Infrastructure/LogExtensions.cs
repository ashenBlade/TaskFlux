using Raft.Core.Log;

namespace Raft.Core.Infrastructure;

public static class LogExtensions
{
    public static bool IsEmpty(this ILog log)
    {
        return log.LastLogEntry == LogEntry.Empty;
    }
}