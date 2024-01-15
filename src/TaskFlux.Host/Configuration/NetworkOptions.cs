using Microsoft.Extensions.Configuration;

namespace TaskFlux.Host.Configuration;

public class NetworkOptions
{
    private static TimeSpan DefaultClientRequestTimeout => TimeSpan.FromSeconds(5);
    public TimeSpan ClientRequestTimeout { get; init; } = DefaultClientRequestTimeout;

    public static NetworkOptions FromConfiguration(IConfiguration configuration)
    {
        return new NetworkOptions()
        {
            ClientRequestTimeout = configuration.GetValue(nameof(ClientRequestTimeout), DefaultClientRequestTimeout)
        };
    }
}