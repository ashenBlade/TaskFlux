using Microsoft.Extensions.Configuration;

namespace Raft.Server.Options;

public class NetworkOptions
{
    public static NetworkOptions Default => new();
    
    [ConfigurationKeyName("CONNECTION_TIMEOUT")]
    public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromMilliseconds(500);

    [ConfigurationKeyName("REQUEST_TIMEOUT")]
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(1);
}