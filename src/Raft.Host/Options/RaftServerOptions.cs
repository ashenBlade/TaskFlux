using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;

namespace Raft.Host.Options;

public class RaftServerOptions
{
    [ConfigurationKeyName("PEERS")]
    public PeerInfo[] Peers { get; set; } = Array.Empty<PeerInfo>(); 
    
    [Required]
    [ConfigurationKeyName("NODE_ID")]
    public int NodeId { get; set; }

    [Required]
    [ConfigurationKeyName("LISTEN_PORT")]
    public int Port { get; set; }

    [Required]
    [ConfigurationKeyName("LISTEN_HOST")]
    public string Host { get; set; } = "localhost";

    [ConfigurationKeyName("RECEIVE_BUFFER_SIZE")]
    public int ReceiveBufferSize { get; set; } = 128;

    [ConfigurationKeyName("ELECTION_TIMEOUT")]
    public TimeSpan ElectionTimeout { get; set; } = TimeSpan.FromSeconds(5);

    [Required]
    [ConfigurationKeyName("LOG_FILE")]
    public string LogFile { get; set; } = null!;


    [Required]
    [ConfigurationKeyName("METADATA_FILE")]
    public string MetadataFile { get; set; } = null!;
}