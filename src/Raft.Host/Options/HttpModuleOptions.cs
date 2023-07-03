using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;

namespace Raft.Host.Options;

public class HttpModuleOptions
{
    /// <summary>
    /// Порт для биндинга
    /// </summary>
    [ConfigurationKeyName("PORT")]
    public int Port { get; set; } = 2602;
}