using Microsoft.Extensions.Configuration;
using TaskFlux.Transport.Http;
using TaskFlux.Transport.Tcp;

namespace TaskFlux.Host.Configuration;

public class ApplicationOptions
{
    public ClusterOptions Cluster { get; private set; } = new();
    public NetworkOptions Network { get; private set; } = new();
    public HttpAdapterOptions Http { get; private set; } = new();
    public TcpAdapterOptions TcpModule { get; private set; } = new();


    /// <summary>
    /// Создать новую конфигурацию приложения, используя переданные аргументы
    /// </summary>
    public static ApplicationOptions FromConfiguration(IConfigurationRoot configuration)
    {
        return new ApplicationOptions()
        {
            Cluster = ClusterOptions.FromConfiguration(configuration),
            Network = NetworkOptions.FromConfiguration(configuration),
            Http = configuration.Get<HttpAdapterOptions>(),
            TcpModule = configuration.Get<TcpAdapterOptions>()
        };
    }
}