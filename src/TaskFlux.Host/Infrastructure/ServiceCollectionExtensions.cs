using Microsoft.Extensions.DependencyInjection;
using Serilog;
using TaskFlux.Application;
using TaskFlux.Consensus;
using TaskFlux.Core.Commands;
using TaskFlux.Host.Configuration;
using TaskFlux.Transport.Http;
using TaskFlux.Transport.Tcp;

namespace TaskFlux.Host.Infrastructure;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddNodeStateObserverHostedService(this IServiceCollection sc,
                                                                       RaftConsensusModule<Command, Response> module)
    {
        return sc.AddHostedService(_ =>
            new NodeStateObserverBackgroundService(module, TimeSpan.FromSeconds(5),
                Log.ForContext("SourceContext", "StateObserver")));
    }

    public static IServiceCollection AddTcpRequestModule(this IServiceCollection sc)
    {
        return sc.AddHostedService(sp => new TcpAdapterBackgroundService(sp.GetRequiredService<IRequestAcceptor>(),
            sp.GetRequiredService<ApplicationOptions>().TcpModule,
            sp.GetRequiredService<IApplicationInfo>(),
            Log.ForContext<TcpAdapterBackgroundService>()));
    }

    public static IServiceCollection AddHttpRequestModule(this IServiceCollection sc)
    {
        return sc.AddHostedService(sp =>
        {
            var options = sp.GetRequiredService<ApplicationOptions>().Http;
            var service =
                new HttpAdapterBackgroundService(options.HttpAdapterListenPort,
                    Log.ForContext<HttpAdapterBackgroundService>());
            service.AddHandler(HttpMethod.Post, "/command",
                new SubmitCommandRequestHandler(sp.GetRequiredService<IRequestAcceptor>(),
                    sp.GetRequiredService<IApplicationInfo>(),
                    Log.ForContext<SubmitCommandRequestHandler>()));
            return service;
        });
    }
}