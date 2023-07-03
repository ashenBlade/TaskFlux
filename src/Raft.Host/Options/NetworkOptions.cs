using Microsoft.Extensions.Configuration;

namespace Raft.Host.Options;

public class NetworkOptions
{
    public static NetworkOptions Default => new();
    
    [ConfigurationKeyName("CONNECTION_TIMEOUT")]
    public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(2);

    [ConfigurationKeyName("REQUEST_TIMEOUT")]
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(1);
}