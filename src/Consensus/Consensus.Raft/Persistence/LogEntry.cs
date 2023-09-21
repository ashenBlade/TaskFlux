namespace Consensus.Raft.Persistence;

public readonly record struct LogEntry(Term Term, byte[] Data);
