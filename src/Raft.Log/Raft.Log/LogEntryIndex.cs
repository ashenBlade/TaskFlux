using Raft.Core;
using Raft.Core.Log;

namespace Raft.Log;

public readonly record struct LogEntryIndex(int Index, LogEntry Entry)
{
    public Term Term => Entry.Term;
    public byte[] Data => Entry.Data;
}