using System.ComponentModel.DataAnnotations;
using System.Net;
using Microsoft.Extensions.Configuration;

namespace TaskFlux.Host.Options;

public class RaftServerOptions
{
    /// <summary>
    /// Массив адресов всех узлов кластера, включая текущий с соответвием индекса его Id
    /// </summary>
    [ConfigurationKeyName("PEERS")]
    public string[] Peers { get; set; } = Array.Empty<string>();

    /// <summary>
    /// Id текущего узла
    /// </summary>
    [Required]
    [ConfigurationKeyName("NODE_ID")]
    [Range(0, int.MaxValue, ErrorMessage = "Id узла не может быть отрицательным")]
    public int NodeId { get; set; }

    [Required]
    [ConfigurationKeyName("LISTEN_PORT")]
    [Range(IPEndPoint.MinPort, IPEndPoint.MaxPort)]
    public int Port { get; set; }

    [Required]
    [ConfigurationKeyName("LISTEN_HOST")]
    public string Host { get; set; } = "localhost";

    [ConfigurationKeyName("RECEIVE_BUFFER_SIZE")]
    public int ReceiveBufferSize { get; set; } = 512;

    [ConfigurationKeyName("ELECTION_TIMEOUT")]
    public TimeSpan ElectionTimeout { get; set; } = TimeSpan.FromSeconds(5);

    [ConfigurationKeyName("DATA_DIR")]
    public string? DataDirectory { get; set; } = "/var/lib/tflux";
}