using Microsoft.Extensions.Configuration;

namespace TaskFlux.Host.Modules.HttpRequest;

public class HttpRequestModuleOptions
{
    public static HttpRequestModuleOptions Default => new();

    /// <summary>
    /// Порт для биндинга
    /// </summary>
    [ConfigurationKeyName("PORT")]
    public int Port { get; set; } = 1606;
}