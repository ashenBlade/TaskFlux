using System.Net;
using System.Net.Sockets;
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

    public static IWebHostBuilder ConfigureTaskFluxKestrel(this IWebHostBuilder web, HttpOptions httpOptions,
        GrpcOptions grpcOptions)
    {
        web.ConfigureServices(sp =>
        {
            sp.Configure<KestrelServerOptions>(kestrel =>
            {
                if (httpOptions.HttpEnabled)
                {
                    ConfigureHttp(kestrel, httpOptions);
                }
                else
                {
                    Log.Information("Пропускаю настройку HTTP модуля");
                }


                if (grpcOptions.GrpcEnabled)
                {
                    ConfigureGrpc(kestrel, grpcOptions);
                }
                else
                {
                    Log.Information("Пропускаю настройку gRPC модуля");
                }

                var kestrelOptions = new ConfigurationBuilder()
                    .AddJsonFile("kestrel.settings.json", optional: true)
                    .Build();
                kestrel.Configure(kestrelOptions, reloadOnChange: true);
            });
        });
        return web;
    }

    private static void ConfigureHttp(KestrelServerOptions kestrel, HttpOptions http)
    {
        const int defaultHttpPort = 1606;
        var addressesToBind = ExtractIpEndpoints(http.HttpListenAddress ?? "localhost", defaultHttpPort);
        Log.Information("Адреса для HTTP запросов: {HttpEndpoints}", addressesToBind);
        foreach (var ep in addressesToBind)
        {
            kestrel.Listen(ep);
        }
    }

    private static void ConfigureGrpc(KestrelServerOptions kestrel, GrpcOptions grpc)
    {
        const int defaultGrpcPort = 1607;
        var toBind = ExtractIpEndpoints(grpc.GrpcListenAddress ?? "localhost", defaultGrpcPort);
        Log.Information("Адреса для gRPC запросов: {GrpcEndpoints}", toBind);
        foreach (var ip in toBind)
        {
            kestrel.Listen(ip, static l =>
            {
                // gRPC использует HTTP2
                l.Protocols = HttpProtocols.Http2;
            });
        }
    }

    private static List<IPEndPoint> ExtractIpEndpoints(string address, int defaultPort)
    {
        var addressesToBind = new List<IPEndPoint>();
        if (IPEndPoint.TryParse(address, out var ipEndPoint))
        {
            if (ipEndPoint.Port == 0)
            {
                // Скорее всего 0 означает, что порт не был указан
                ipEndPoint.Port = defaultPort;
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
                port = defaultPort;
            }

            var hostNameOrAddress = parts[0];
            var addresses = Dns.GetHostAddresses(hostNameOrAddress)
                .Where(ip => ip.AddressFamily == AddressFamily.InterNetwork).ToArray();
            foreach (var ip in addresses)
            {
                addressesToBind.Add(new IPEndPoint(ip, port));
            }
        }

        return addressesToBind;
    }
}