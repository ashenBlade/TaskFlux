using TaskFlux.Transport.Tcp;

namespace TaskFlux.Host.Configuration;

public class ApplicationOptions
{
    public ClusterOptions Cluster { get; private set; } = new();
    public NetworkOptions Network { get; private set; } = new();
    public HttpOptions Http { get; private set; } = new();
    public GrpcOptions Grpc { get; private set; } = new();
    public TcpAdapterOptions TcpModule { get; private set; } = new();
    public LoggingOptions Logging { get; private set; } = new();
    public PersistenceOptions Persistence { get; private set; } = new();

    /// <summary>
    /// Создать новую конфигурацию приложения, используя переданные аргументы
    /// </summary>
    public static ApplicationOptions FromConfiguration(IConfigurationRoot configuration)
    {
        return new ApplicationOptions()
        {
            Cluster = ClusterOptions.FromConfiguration(configuration),
            Network = NetworkOptions.FromConfiguration(configuration),
            Http = HttpOptions.FromConfiguration(configuration),
            TcpModule = TcpAdapterOptions.FromConfiguration(configuration),
            Persistence = PersistenceOptions.FromConfiguration(configuration),
            Logging = LoggingOptions.FromConfiguration(configuration),
            Grpc = GrpcOptions.FromConfiguration(configuration),
        };
    }
}