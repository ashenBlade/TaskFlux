using Microsoft.Extensions.Configuration;
using Serilog.Events;

namespace TaskFlux.Host.Configuration;

public class LoggingOptions
{
    public const LogEventLevel DefaultLogLevel = LogEventLevel.Information;
    public LogEventLevel LogLevel { get; set; } = DefaultLogLevel;

    public static LoggingOptions FromConfiguration(IConfiguration configuration)
    {
        return new LoggingOptions() {LogLevel = configuration.GetValue(nameof(LogLevel), DefaultLogLevel)};
    }
}