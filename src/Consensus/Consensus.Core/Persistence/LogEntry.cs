namespace Consensus.Core.Log;

public readonly record struct LogEntry(Term Term, byte[] Data);
