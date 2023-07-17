using Consensus.Core.Log;

namespace Consensus.Log;

public static class LogEntryExtensions
{
    public static LogEntry ClearData(this LogEntry entry) => entry with
    {
        Data = Array.Empty<byte>()
    };
}