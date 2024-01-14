using Consensus.Raft;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Host.Configuration;
using TaskFlux.Transport.Common;
using TaskFlux.Transport.Http;
using TaskFlux.Transport.Tcp;

namespace TaskFlux.Host.Infrastructure;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddNodeStateObserverHostedService(this IServiceCollection sc)
    {
        return sc.AddHostedService(sp =>
            new NodeStateObserverBackgroundService(sp.GetRequiredService<IRaftConsensusModule<Command, Response>>(),
                TimeSpan.FromSeconds(5), Log.ForContext("SourceContext", "StateObserver")));
    }

    public static IServiceCollection AddTcpRequestModule(this IServiceCollection sc)
    {
        return sc.AddHostedService(sp => new SocketRequestModule(sp.GetRequiredService<IRequestAcceptor>(),
            sp.GetRequiredService<ApplicationOptions>().TcpModule,
            sp.GetRequiredService<IApplicationInfo>(),
            Log.ForContext<SocketRequestModule>()));
    }

    public static IServiceCollection AddHttpRequestModule(this IServiceCollection sc)
    {
        return sc.AddHostedService(sp =>
        {
            var options = sp.GetRequiredService<ApplicationOptions>().Http;
            var service =
                new HttpRequestBackgroundService(options.HttpPort, Log.ForContext<HttpRequestBackgroundService>());
            service.AddHandler(HttpMethod.Post, "/command",
                new SubmitCommandRequestHandler(sp.GetRequiredService<IRequestAcceptor>(),
                    sp.GetRequiredService<IApplicationInfo>(),
                    Log.ForContext<SubmitCommandRequestHandler>()));
            return service;
        });
    }
}