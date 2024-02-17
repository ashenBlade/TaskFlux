using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Serilog;
using TaskFlux.Application;
using TaskFlux.Consensus;
using TaskFlux.Core.Commands;
using TaskFlux.Host.Configuration;
using TaskFlux.Persistence;
using TaskFlux.Transport.Tcp;
using IApplicationLifetime = TaskFlux.Application.IApplicationLifetime;

namespace TaskFlux.Host.Infrastructure;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddNodeStateObserverHostedService(this IServiceCollection sc,
                                                                       FileSystemPersistenceFacade persistence,
                                                                       RaftConsensusModule<Command, Response> module)
    {
        return sc.AddHostedService(_ =>
            new NodeStateObserverBackgroundService(module, persistence, TimeSpan.FromSeconds(5),
                Log.ForContext("SourceContext", "StateObserver")));
    }

    public static IServiceCollection AddTcpRequestModule(this IServiceCollection sc, IApplicationLifetime lifetime)
    {
        return sc.AddHostedService(sp => new TcpAdapterBackgroundService(sp.GetRequiredService<IRequestAcceptor>(),
            sp.GetRequiredService<ApplicationOptions>().TcpModule,
            sp.GetRequiredService<IApplicationInfo>(),
            lifetime,
            Log.ForContext<TcpAdapterBackgroundService>()));
    }

    public static IWebHostBuilder ConfigureTaskFluxKestrel(this IWebHostBuilder web, HttpOptions httpOptions)
    {
        web.UseKestrel();
        web.ConfigureServices(sp =>
        {
            sp.Configure<KestrelServerOptions>(kestrel =>
            {
                if (!string.IsNullOrWhiteSpace(httpOptions.HttpListenAddress))
                {
                    const int defaultHttpPort = 1606;
                    var address = httpOptions.HttpListenAddress;
                    var addressesToBind = new List<IPEndPoint>();
                    if (IPEndPoint.TryParse(address, out var ipEndPoint))
                    {
                        if (ipEndPoint.Port == 0)
                        {
                            // Скорее всего 0 означает, что порт не был указан
                            Log.Information("Порт для HTTP запросов не указан. Выставляю в {DefaultHttpPort}",
                                defaultHttpPort);
                            ipEndPoint.Port = defaultHttpPort;
                        }

                        addressesToBind.Add(ipEndPoint);
                    }
                    else
                    {
                        // Если адрес - не IP, то парсим как DNS хост
                        var parts = address.Split(':');
                        int port;
                        if (parts.Length != 1)
                        {
                            port = int.Parse(parts[1]);
                        }
                        else
                        {
                            Log.Information("Порт для HTTP запросов не указан. Выставляю в {DefaultHttpPort}",
                                defaultHttpPort);
                            port = defaultHttpPort;
                        }

                        var hostNameOrAddress = parts[0];
                        Log.Debug("Ищу IP адреса для хоста {HostName}", hostNameOrAddress);
                        var addresses = Dns.GetHostAddresses(hostNameOrAddress);
                        Log.Debug("Найденные адреса для хоста: {IPAddresses}", addresses);
                        foreach (var ip in addresses)
                        {
                            addressesToBind.Add(new IPEndPoint(ip, port));
                        }
                    }

                    foreach (var ep in addressesToBind)
                    {
                        kestrel.Listen(ep);
                    }
                }

                var kestrelOptions = new ConfigurationBuilder()
                                    .AddJsonFile("kestrel.settings.json", optional: true)
                                    .Build();
                kestrel.Configure(kestrelOptions, reloadOnChange: true);
            });
        });
        return web;
    }
}