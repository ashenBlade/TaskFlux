using System.ComponentModel.DataAnnotations;
using System.Net;
using Microsoft.Extensions.Configuration;
using Utils.Network;

namespace TaskFlux.Host.Configuration;

public class ClusterOptions
{
    /// <summary>
    /// Массив адресов всех узлов кластера, включая текущий с соответствием индекса его Id
    /// </summary>
    [MinLength(1)]
    public EndPoint[] Peers { get; set; } = Array.Empty<EndPoint>();

    /// <summary>
    /// Id текущего узла
    /// </summary>
    [Required]
    [Range(0, int.MaxValue, ErrorMessage = "Id узла не может быть отрицательным")]
    public int NodeId { get; set; }

    [Required]
    [Range(IPEndPoint.MinPort, IPEndPoint.MaxPort)]
    public int ListenPort { get; set; }

    [Required]
    public string ListenHost { get; set; } = "localhost";

    [Range(1, int.MaxValue)]
    public int ReceiveBufferSize { get; set; } = 512;

    [Required]
    public string DataDirectory { get; set; } = "/var/lib/tflux";

    public static ClusterOptions FromConfiguration(IConfiguration configuration)
    {
        var value = configuration.Get<ClusterOptions>()
                 ?? new ClusterOptions();
        var endpointsArrayString = configuration.GetValue<string>(nameof(Peers))
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

        value.Peers = peers.ToArray();
        return value;
    }
}