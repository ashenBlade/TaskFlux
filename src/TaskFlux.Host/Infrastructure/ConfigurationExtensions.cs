using Microsoft.Extensions.Configuration;

namespace TaskFlux.Host.Infrastructure;

public static class ConfigurationExtensions
{
    public static string GetString(this IConfiguration configuration, string name, string? defaultValue = null) =>
        configuration.GetValue<string>(name) ?? ( defaultValue ?? string.Empty );
}