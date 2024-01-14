using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileProviders;
using TaskFlux.Host.Configuration.Providers;

namespace TaskFlux.Host.Infrastructure;

public static class ConfigurationBuilderExtensions
{
    public static IConfigurationBuilder AddTaskFluxSource(this IConfigurationBuilder builder,
                                                          string[] args,
                                                          string jsonFilePath)
    {
        builder.Add(new TaskFluxCommandLineConfigurationSource(args))
               .Add(new TaskFluxEnvironmentVariablesConfigurationSource())
               .AddJsonFile(config =>
                {
                    config.Path = jsonFilePath;
                    config.Optional = true;
                    config.ReloadOnChange = true;
                    config.FileProvider = new PhysicalFileProvider(Directory.GetCurrentDirectory());
                });
        return builder;
    }
}