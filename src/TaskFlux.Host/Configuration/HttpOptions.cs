namespace TaskFlux.Host.Configuration;

public class HttpOptions
{
    public string? HttpListenAddress { get; set; }
    public bool HttpEnabled { get; set; } = true;

    public static HttpOptions FromConfiguration(IConfiguration configuration)
    {
        return new HttpOptions()
        {
            HttpListenAddress = configuration.GetValue<string?>(nameof(HttpListenAddress)),
            HttpEnabled = configuration.GetValue<bool>(nameof(HttpEnabled), true),
        };
    }
}