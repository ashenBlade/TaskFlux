using Consensus.Core;
using Consensus.Core.Log;

namespace Consensus.Log;

public readonly record struct LogEntryIndex(int Index, LogEntry Entry)
{
    public Term Term => Entry.Term;
    public byte[] Data => Entry.Data;
}