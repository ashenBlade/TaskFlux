namespace TaskFlux.Host.Configuration;

public class GrpcOptions
{
    public string? GrpcListenAddress { get; set; }
    public bool GrpcEnabled { get; set; } = true;

    public static GrpcOptions FromConfiguration(IConfiguration configuration)
    {
        return new GrpcOptions()
        {
            GrpcListenAddress = configuration.GetValue<string?>(nameof(GrpcListenAddress)),
            GrpcEnabled = configuration.GetValue<bool>(nameof(GrpcEnabled), true),
        };
    }
}