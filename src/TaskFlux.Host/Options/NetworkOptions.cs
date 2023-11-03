using Microsoft.Extensions.Configuration;

namespace TaskFlux.Host.Options;

public class NetworkOptions
{
    public static NetworkOptions Default => new();

    [ConfigurationKeyName("REQUEST_TIMEOUT")]
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(3);
}