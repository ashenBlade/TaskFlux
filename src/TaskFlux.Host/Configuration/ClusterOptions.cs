using System.ComponentModel.DataAnnotations;
using System.Net;
using Microsoft.Extensions.Configuration;
using TaskFlux.Host.Infrastructure;
using TaskFlux.Utils.Network;

namespace TaskFlux.Host.Configuration;

public class ClusterOptions
{
    /// <summary>
    /// Массив адресов всех узлов кластера, включая текущий с соответствием индекса его Id
    /// </summary>
    [MinLength(1)]
    public EndPoint[] ClusterPeers { get; init; } = Array.Empty<EndPoint>();

    public const int DefaultNodeId = 0;

    /// <summary>
    /// Id текущего узла
    /// </summary>
    [Required]
    [Range(0, int.MaxValue, ErrorMessage = "Id узла не может быть отрицательным")]
    public int ClusterNodeId { get; init; } = DefaultNodeId;

    public const int DefaultClusterListenPort = 5000;

    [Required]
    [Range(IPEndPoint.MinPort, IPEndPoint.MaxPort)]
    public int ClusterListenPort { get; init; } = DefaultClusterListenPort;

    private const string DefaultClusterListenHost = "localhost";

    [Required]
    public string ClusterListenHost { get; init; } = DefaultClusterListenHost;

    public const int DefaultReceiveBufferSize = 2048;

    [Range(1, int.MaxValue)]
    public int ClusterReceiveBufferSize { get; init; } = DefaultReceiveBufferSize;

    public const string DefaultDataDirectory = "/var/lib/tflux";

    [Required]
    public string ClusterDataDirectory { get; init; } = DefaultDataDirectory;

    public static TimeSpan DefaultRequestTimeout => TimeSpan.FromSeconds(5);

    public TimeSpan ClusterRequestTimeout { get; init; } = DefaultRequestTimeout;

    public static ClusterOptions FromConfiguration(IConfiguration configuration)
    {
        var endpointsArrayString = configuration.GetValue<string>(nameof(ClusterPeers))
                                ?? throw new ArgumentException("Не удалось получить адреса узлов кластера");

        var endpointsRaw = endpointsArrayString.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var peers = new List<EndPoint>();
        foreach (var ep in endpointsRaw)
        {
            try
            {
                peers.Add(EndPointHelpers.ParseEndPoint(ep));
            }
            catch (Exception e)
            {
                throw new ArgumentException($"Ошибка во время парсинга адреса {endpointsRaw}", e);
            }
        }

        if (peers.Count == 0)
        {
            throw new ArgumentException("В конфигурации не указано ни одного адреса узла кластера");
        }

        return new ClusterOptions()
        {
            ClusterPeers = peers.ToArray(),
            ClusterDataDirectory = configuration.GetString(nameof(ClusterDataDirectory), DefaultDataDirectory),
            ClusterListenHost = configuration.GetString(nameof(ClusterListenHost), DefaultClusterListenHost),
            ClusterListenPort = configuration.GetValue(nameof(ClusterListenPort), DefaultClusterListenPort),
            ClusterNodeId = configuration.GetValue(nameof(ClusterNodeId), 0),
            ClusterRequestTimeout = configuration.GetValue(nameof(ClusterRequestTimeout), DefaultRequestTimeout),
            ClusterReceiveBufferSize =
                configuration.GetValue(nameof(ClusterReceiveBufferSize), DefaultReceiveBufferSize)
        };
    }
}