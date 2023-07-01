namespace Raft.Host;

public record CancellableThreadParameter<T>(T Value, CancellationToken Token);