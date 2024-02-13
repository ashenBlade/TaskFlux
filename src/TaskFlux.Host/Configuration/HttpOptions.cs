namespace TaskFlux.Host.Configuration;

public class HttpOptions
{
    public string? HttpListenAddress { get; set; }

    public static HttpOptions FromConfiguration(IConfiguration configuration)
    {
        return new HttpOptions() {HttpListenAddress = configuration.GetValue<string?>(nameof(HttpListenAddress)),};
    }
}