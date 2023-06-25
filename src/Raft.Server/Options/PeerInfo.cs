using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;

namespace Raft.Server.Options;

public record PeerInfo
{
    [Required]
    [ConfigurationKeyName("PORT")]
    public int Port { get; set; }

    [Required]
    [ConfigurationKeyName("HOST")]
    public string Host { get; set; } = null!;
    [Required]
    [ConfigurationKeyName("ID")]
    public int Id { get; set; }

}