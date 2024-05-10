using System.ComponentModel.DataAnnotations;
using System.IO.Abstractions;
using System.Net;
using System.Runtime.InteropServices;
using Serilog;
using Serilog.Core;
using TaskFlux.Application;
using TaskFlux.Application.Cluster;
using TaskFlux.Application.Cluster.Network;
using TaskFlux.Application.RecordAwaiter;
using TaskFlux.Consensus;
using TaskFlux.Consensus.Timers;
using TaskFlux.Core;
using TaskFlux.Core.Commands;
using TaskFlux.Host;
using TaskFlux.Host.Configuration;
using TaskFlux.Host.Infrastructure;
using TaskFlux.Persistence;
using TaskFlux.Persistence.Log;
using TaskFlux.Persistence.Snapshot;
using TaskFlux.Transport.Grpc;
using TaskFlux.Transport.Http;
using TaskFlux.Utils.Network;

var lifetime = new HostApplicationLifetime();

Console.CancelKeyPress += (_, args) =>
{
    Log.Information("Получен Ctrl + C. Посылаю сигнал остановки приложения");
    lifetime.Stop();
    args.Cancel = true;
};

var logLevelSwitch = new LoggingLevelSwitch();

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.ControlledBy(logLevelSwitch)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .Enrich.WithProperty("SourceContext", "Main") // Это для глобального логера
    .CreateBootstrapLogger();

PosixSignalRegistration? sigTermRegistration = null;
try
{
    // Стандартный сигнал для завершения работы приложения.
    // Докер посылает его для остановки
    sigTermRegistration = PosixSignalRegistration.Create(PosixSignal.SIGTERM, context =>
    {
        Log.Information("Получен SIGTERM. Посылаю сигнал остановки приложения");
        lifetime.Stop();
        context.Cancel = true;
    });
}
catch (NotSupportedException e)
{
    Log.Warning(e, "Не удалось зарегистрировать обработчик SIGTERM: платформа не поддерживает");
}

try
{
    var configuration = new ConfigurationBuilder()
        .AddTaskFluxSource(args, "taskflux.settings.json")
        .Build();

    var options = ApplicationOptions.FromConfiguration(configuration);

    if (!TryValidateOptions(options))
    {
        return 1;
    }

    logLevelSwitch.MinimumLevel = options.Logging.LogLevel;

    var nodeId = new NodeId(options.Cluster.ClusterNodeId);

    using var persistence = InitializePersistence(options.Persistence);

    DumpDataState(persistence);

    var peers = ExtractPeers(options.Cluster, nodeId, options.Network);

    using var jobQueue = CreateJobQueue(options, lifetime);
    using var consensusModule = CreateRaftConsensusModule(nodeId, peers, persistence, jobQueue);
    using var requestAcceptor = CreateRequestAcceptor(consensusModule, lifetime);
    using var connectionManager = CreateClusterNodeConnectionManager(options, consensusModule, lifetime);
    var taskFluxHostedService =
        CreateTaskFluxHostedService(consensusModule, requestAcceptor, jobQueue, connectionManager);

    try
    {
        using var host = BuildHost();
        Log.Debug("Запускаю хост");
        await host.RunAsync(lifetime.Token);
        lifetime.Stop();
    }
    catch (Exception e)
    {
        Log.Fatal(e, "Во время работы приложения поймано необработанное исключение");
        lifetime.StopAbnormal();
    }

    IHost BuildHost()
    {
        return new HostBuilder()
            .ConfigureHostConfiguration(host =>
                host.AddEnvironmentVariables("DOTNET_")
                    .AddCommandLine(args))
            .ConfigureAppConfiguration(app =>
                app.AddEnvironmentVariables()
                    .AddCommandLine(args)
                    .AddJsonFile("appsettings.json", optional: true))
            .ConfigureWebHost(web =>
            {
                web.UseKestrel();
                web.ConfigureTaskFluxKestrel(options.Http, options.Grpc);
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(ep =>
                    {
                        var opts = ep.ServiceProvider.GetRequiredService<ApplicationOptions>();

                        ep.MapPrometheusScrapingEndpoint("/metrics");
                        if (opts.Http.HttpEnabled)
                        {
                            ep.MapControllers();
                        }

                        if (opts.Grpc.GrpcEnabled)
                        {
                            ep.MapGrpcService<TaskFluxGrpcService>();
                        }
                    });
                });
            })
            .UseSerilog((ctx, serilog) =>
            {
                serilog
                    .MinimumLevel.ControlledBy(logLevelSwitch)
                    .Enrich.FromLogContext()
                    .WriteTo.Console()
                    .Enrich.WithProperty("SourceContext", "Main")
                    .ReadFrom.Configuration(ctx.Configuration);
            })
            .ConfigureServices((_, services) =>
            {
                services.AddOpenTelemetry()
                    .WithMetrics(metrics => { metrics.AddTaskFluxMetrics(persistence, requestAcceptor); });

                services.AddHostedService(_ => taskFluxHostedService);

                services.AddSingleton<IRequestAcceptor>(requestAcceptor);
                services.AddSingleton(options);
                services.AddSingleton<IApplicationInfo>(sp => new ProxyApplicationInfo(consensusModule,
                    sp.GetRequiredService<ApplicationOptions>().Cluster.ClusterPeers));

                services.AddNodeStateObserverHostedService(persistence, consensusModule);

                // Модуль для HTTP запросов
                services.AddControllers()
                    .AddApplicationPart(typeof(RequestController).Assembly);

                // Модуль для запросов по своему протоколу
                services.AddTcpRequestModule(lifetime);

                // Модуль gRPC запросов
                services.AddGrpc();
            })
            .Build();
    }
}
catch (Exception e)
{
    Log.Fatal(e, "Необработанное исключение во время настройки сервера");
    lifetime.StopAbnormal();
}
finally
{
    sigTermRegistration?.Dispose();
    Log.CloseAndFlush();
}

Log.Information("Приложение закрывается");
return await lifetime.WaitReturnCode();

FileSystemPersistenceFacade InitializePersistence(PersistenceOptions options)
{
    var fs = new FileSystem();
    var dataDirectoryInfo = fs.DirectoryInfo.New(Path.Combine(options.DataDirectory, "data"));
    var snapshotOptions = new SnapshotOptions(
        snapshotCreationSegmentsThreshold: options.SnapshotCreationSegmentsThreshold);
    var logOptions = new SegmentedFileLogOptions(softLimit: options.LogFileSoftLimit,
        hardLimit: options.LogFileHardLimit,
        preallocateSegment: true,
        maxReadEntriesSize: options.ReplicationMaxSendSize);
    return FileSystemPersistenceFacade.Initialize(dataDirectory: dataDirectoryInfo,
        logger: Log.ForContext<FileSystemPersistenceFacade>(),
        snapshotOptions: snapshotOptions,
        logOptions: logOptions);
}

RaftConsensusModule<Command, Response> CreateRaftConsensusModule(NodeId nodeId,
    IPeer[] peers,
    FileSystemPersistenceFacade persistence,
    IBackgroundJobQueue jobQueue)
{
    var logger = Log.Logger.ForContext("SourceContext", "ConsensusModule");
    var deltaExtractor = new TaskFluxDeltaExtractor();
    var peerGroup = new PeerGroup(peers);
    var timerFactory = new ThreadingTimerFactory(lower: TimeSpan.FromMilliseconds(1500),
        upper: TimeSpan.FromMilliseconds(2500),
        heartbeatTimeout: TimeSpan.FromMilliseconds(1000));
    var applicationFactory = new TaskFluxApplicationFactory(new ValueTaskSourceQueueSubscriberManagerFactory(128));

    return RaftConsensusModule<Command, Response>.Create(nodeId,
        peerGroup, logger, timerFactory,
        jobQueue, persistence,
        deltaExtractor, applicationFactory);
}

static IPeer[] ExtractPeers(ClusterOptions serverOptions, NodeId currentNodeId, NetworkOptions networkOptions)
{
    var peers = new IPeer[serverOptions.ClusterPeers.Length - 1]; // Все кроме себя
    var connectionErrorDelay = TimeSpan.FromMilliseconds(100);

    // Все до текущего узла
    for (var i = 0; i < currentNodeId.Id; i++)
    {
        var endpoint = serverOptions.ClusterPeers[i];
        var id = new NodeId(i);
        peers[i] = CreatePeer(id, endpoint);
    }

    // Все после текущего узла
    for (var i = currentNodeId.Id + 1; i < serverOptions.ClusterPeers.Length; i++)
    {
        var endpoint = serverOptions.ClusterPeers[i];
        var id = new NodeId(i);
        peers[i - 1] = CreatePeer(id, endpoint);
    }

    return peers;

    IPeer CreatePeer(NodeId id, EndPoint endPoint)
    {
        IPeer peer = TcpPeer.Create(currentNodeId, id, endPoint, networkOptions.ClientRequestTimeout,
            connectionErrorDelay, Log.ForContext<TcpPeer>());
        peer = new ExclusiveAccessPeerDecorator(peer);
        return peer;
    }
}

bool TryValidateOptions(ApplicationOptions options)
{
    var results = new List<ValidationResult>();

    _ = Validator.TryValidateObject(options, new ValidationContext(options), results, true);

    if (options.Persistence.LogFileHardLimit < options.Persistence.LogFileSoftLimit)
    {
        results.Add(new ValidationResult(
            $"Жесткий предел размера файла не может быть меньше мягкого предела. Мягкий предел: {options.Persistence.LogFileSoftLimit}. Жесткий предел: {options.Persistence.LogFileHardLimit}",
            new[] { nameof(options.Persistence.LogFileHardLimit), nameof(options.Persistence.LogFileSoftLimit) }));
    }

    if (options.Http.HttpListenAddress is { Length: > 0 } httpListenAddress)
    {
        try
        {
            _ = EndPointHelpers.ParseEndPoint(httpListenAddress, 0);
        }
        catch (ArgumentException ae)
        {
            results.Add(new ValidationResult($"Невалидный адрес для HTTP запросов: {ae.Message}",
                new[] { nameof(options.Http.HttpListenAddress) }));
        }
    }

    if (results.Count == 0)
    {
        return true;
    }

    Log.Fatal("Ошибка валидации конфигурации. Обнаружены ошибки: {Errors}", results.Select(r => r.ErrorMessage));
    return false;
}

void DumpDataState(FileSystemPersistenceFacade persistence)
{
    Log.Information("Последняя команда состояния: {LastEntry}", persistence.LastEntry);
    if (persistence.TryGetSnapshotLastEntryInfo(out var lastApplied))
    {
        Log.Information("Последняя запись в снапшоте: {LastSnapshotEntry}", lastApplied);
    }
    else
    {
        Log.Information("Снапшот отсутствует");
    }

    Log.Information("Количество записей в логе: {LogEntriesCount}", persistence.Log.Count);
    if (!persistence.CommitIndex.IsTomb)
    {
        Log.Information("Индекс закоммиченной команды: {CommitIndex}", persistence.CommitIndex);
    }
}

ThreadPerWorkerBackgroundJobQueue CreateJobQueue(ApplicationOptions applicationOptions,
    HostApplicationLifetime hostApplicationLifetime)
{
    return new ThreadPerWorkerBackgroundJobQueue(applicationOptions.Cluster.ClusterPeers.Length,
        applicationOptions.Cluster.ClusterNodeId,
        Log.ForContext("SourceContext", "BackgroundJobQueue"), hostApplicationLifetime);
}

ExclusiveRequestAcceptor CreateRequestAcceptor(RaftConsensusModule<Command, Response> raftConsensusModule,
    HostApplicationLifetime lifetime1)
{
    return new ExclusiveRequestAcceptor(raftConsensusModule, lifetime1,
        Log.ForContext("SourceContext", "RequestAcceptor"));
}

NodeConnectionManager CreateClusterNodeConnectionManager(ApplicationOptions applicationOptions,
    RaftConsensusModule<Command, Response> consensusModule,
    HostApplicationLifetime hostApplicationLifetime1)
{
    return new NodeConnectionManager(applicationOptions.Cluster.ClusterListenHost,
        applicationOptions.Cluster.ClusterListenPort,
        consensusModule, applicationOptions.Network.ClientRequestTimeout,
        hostApplicationLifetime1, Log.ForContext<NodeConnectionManager>());
}

TaskFluxNodeHostedService CreateTaskFluxHostedService(RaftConsensusModule<Command, Response> consensusModule,
    ExclusiveRequestAcceptor exclusiveRequestAcceptor,
    ThreadPerWorkerBackgroundJobQueue jobQueue,
    NodeConnectionManager nodeConnectionManager)
{
    return new TaskFluxNodeHostedService(consensusModule, exclusiveRequestAcceptor, jobQueue,
        nodeConnectionManager, Log.ForContext<TaskFluxNodeHostedService>());
}