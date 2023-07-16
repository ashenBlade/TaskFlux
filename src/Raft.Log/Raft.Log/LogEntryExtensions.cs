using Raft.Core.Log;

namespace Raft.Log;

public static class LogEntryExtensions
{
    public static LogEntry ClearData(this LogEntry entry) => entry with
    {
        Data = Array.Empty<byte>()
    };
}