using Microsoft.Extensions.Configuration;

namespace TaskFlux.Host.Modules.HttpRequest;

public class HttpModuleOptions
{
    /// <summary>
    /// Порт для биндинга
    /// </summary>
    [ConfigurationKeyName("PORT")]
    public int Port { get; set; } = 1606;
}