using Microsoft.Extensions.Configuration;
using TaskFlux.Transport.Tcp;
using HttpRequestOptions = TaskFlux.Transport.Http.HttpRequestOptions;

namespace TaskFlux.Host.Configuration;

public class ApplicationOptions
{
    public ClusterOptions Cluster { get; private set; } = new();
    public NetworkOptions Network { get; private set; } = new();
    public HttpRequestOptions Http { get; private set; } = new();
    public SocketRequestModuleOptions TcpModule { get; private set; } = new();


    /// <summary>
    /// Создать новую конфигурацию приложения, используя переданные аргументы
    /// </summary>
    public static ApplicationOptions FromConfiguration(IConfigurationRoot configuration)
    {
        return new ApplicationOptions()
        {
            Cluster = ClusterOptions.FromConfiguration(configuration),
            Network = configuration.Get<NetworkOptions>(),
            Http = configuration.Get<HttpRequestOptions>(),
            TcpModule = configuration.Get<SocketRequestModuleOptions>()
        };
    }
}