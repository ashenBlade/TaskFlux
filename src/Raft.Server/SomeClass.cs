namespace Raft.Server;

public record SomeClass<T>(T Value, CancellationToken Token);