using Microsoft.Extensions.Options;

namespace TaskFlux.Host.Infrastructure;

public class StaticOptionsMonitor<T>: IOptionsMonitor<T>
{
    public T CurrentValue { get; }

    public StaticOptionsMonitor(T value)
    {
        CurrentValue = value;
    }
    
    public T Get(string? name)
    {
        return CurrentValue;
    }

    public IDisposable? OnChange(Action<T, string?> listener)
    {
        return NopeDisposable.Instance;
    }

    private class NopeDisposable : IDisposable
    {
        public static readonly NopeDisposable Instance = new();
        public void Dispose()
        { }
    }

}