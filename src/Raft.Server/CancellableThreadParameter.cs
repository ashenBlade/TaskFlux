namespace Raft.Server;

public record CancellableThreadParameter<T>(T Value, CancellationToken Token);