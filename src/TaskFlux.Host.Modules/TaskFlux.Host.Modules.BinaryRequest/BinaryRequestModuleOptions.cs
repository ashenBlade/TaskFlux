using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;

namespace TaskFlux.Host.Modules.BinaryRequest;

public class BinaryRequestModuleOptions
{
    public static BinaryRequestModuleOptions Default => new();
    public const int DefaultBacklogSize = 128;

    [ConfigurationKeyName("PORT")]
    [Range(1, int.MaxValue)]
    public int Port { get; set; } = 2602;


    [ConfigurationKeyName("BACKLOG_SIZE")]
    [Range(1, int.MaxValue)]
    public int BacklogSize { get; set; } = DefaultBacklogSize;

    /// <summary>
    /// Таймаут простоя клиента.
    /// Время, в которое клиент не посылает никаких запросов
    /// </summary>
    [ConfigurationKeyName("IDLE_TIMEOUT")]
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(1);
}