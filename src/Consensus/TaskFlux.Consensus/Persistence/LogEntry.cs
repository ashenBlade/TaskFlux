namespace TaskFlux.Consensus.Persistence;

public readonly record struct LogEntry(Term Term, byte[] Data);