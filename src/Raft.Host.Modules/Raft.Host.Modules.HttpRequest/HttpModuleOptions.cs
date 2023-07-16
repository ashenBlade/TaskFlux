using Microsoft.Extensions.Configuration;

namespace Raft.Host.Modules.HttpRequest;

public class HttpModuleOptions
{
    /// <summary>
    /// Порт для биндинга
    /// </summary>
    [ConfigurationKeyName("PORT")]
    public int Port { get; set; } = 2602;
}