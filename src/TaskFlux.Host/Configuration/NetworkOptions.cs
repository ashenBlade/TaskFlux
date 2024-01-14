namespace TaskFlux.Host.Configuration;

public class NetworkOptions
{
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(3);
}