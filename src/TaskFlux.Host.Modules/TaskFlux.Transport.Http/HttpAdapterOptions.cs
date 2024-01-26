using Microsoft.Extensions.Configuration;

namespace TaskFlux.Transport.Http;

public class HttpAdapterOptions
{
    public const int DefaultListenPort = 1606;

    /// <summary>
    /// Порт на котором будут прослушиваться HTTP запросы
    /// </summary>
    public int HttpAdapterListenPort { get; set; } = DefaultListenPort;

    public static HttpAdapterOptions FromConfiguration(IConfiguration configuration)
    {
        return new HttpAdapterOptions()
        {
            HttpAdapterListenPort = configuration.GetValue(nameof(HttpAdapterListenPort), DefaultListenPort),
        };
    }
}