namespace Raft.Core.Log;

public readonly record struct LogEntry(Term Term, string Data);
