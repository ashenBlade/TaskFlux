namespace Raft.Host.Infrastructure;

public record CancellableThreadParameter<T>(T Value, CancellationToken Token);