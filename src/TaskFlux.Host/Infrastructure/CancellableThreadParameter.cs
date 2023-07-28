namespace TaskFlux.Host.Infrastructure;

public record CancellableThreadParameter<T>(T Value, CancellationToken Token);