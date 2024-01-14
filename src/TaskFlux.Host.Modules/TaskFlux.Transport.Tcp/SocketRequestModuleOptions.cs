using System.ComponentModel.DataAnnotations;
using System.Net;

namespace TaskFlux.Transport.Tcp;

public class SocketRequestModuleOptions
{
    [Range(IPEndPoint.MinPort, IPEndPoint.MaxPort)]
    public int Port { get; set; } = 2602;

    [Range(1, int.MaxValue)]
    public int BacklogSize { get; set; } = 128;

    /// <summary>
    /// Таймаут простоя клиента.
    /// Время, в которое клиент не посылает никаких запросов
    /// </summary>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(1);
}