using System.ComponentModel.DataAnnotations;
using System.Net;
using Microsoft.Extensions.Configuration;

namespace TaskFlux.Transport.Tcp;

public class TcpAdapterOptions
{
    public const int DefaultListenPort = 2602;

    [Range(IPEndPoint.MinPort, IPEndPoint.MaxPort)]
    public int TcpAdapterListenPort { get; set; } = DefaultListenPort;

    public const int DefaultBacklogSize = 128;

    [Range(1, int.MaxValue)] public int TcpAdapterBacklogSize { get; set; } = DefaultBacklogSize;

    public const int DefaultBufferSize = 2048;

    [Range(1, int.MaxValue)] public int BufferSize { get; set; } = DefaultBufferSize;

    public static TimeSpan DefaultIdleTimeout => TimeSpan.FromMinutes(1);

    /// <summary>
    /// Таймаут простоя клиента.
    /// Время, в которое клиент не посылает никаких запросов
    /// </summary>
    public TimeSpan IdleTimeout { get; set; } = DefaultIdleTimeout;

    public static TcpAdapterOptions FromConfiguration(IConfiguration configuration)
    {
        return new TcpAdapterOptions()
        {
            TcpAdapterListenPort = configuration.GetValue(nameof(TcpAdapterListenPort), DefaultListenPort),
            TcpAdapterBacklogSize = configuration.GetValue(nameof(TcpAdapterBacklogSize), DefaultBacklogSize),
            BufferSize = configuration.GetValue(nameof(BufferSize), DefaultBufferSize),
            IdleTimeout = configuration.GetValue(nameof(IdleTimeout), DefaultIdleTimeout),
        };
    }
}