using System.ComponentModel.DataAnnotations;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Core;
using TaskFlux.Application;
using TaskFlux.Application.Cluster;
using TaskFlux.Consensus;
using TaskFlux.Consensus.Cluster.Network;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.Timers;
using TaskFlux.Core;
using TaskFlux.Core.Commands;
using TaskFlux.Host;
using TaskFlux.Host.Configuration;
using TaskFlux.Host.Infrastructure;

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
            .CreateLogger();

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
    Log.Information(e, "Платформа не поддерживает регистрацию обработчика SIGTERM сигнала");
}

try
{
    var options = GetApplicationOptions();

    if (!TryValidateOptions(options))
    {
        return 1;
    }

    logLevelSwitch.MinimumLevel = options.Logging.LogLevel;

    var nodeId = new NodeId(options.Cluster.ClusterNodeId);

    var facade = InitializePersistence(options.Persistence);
    DumpDataState(facade);

    var peers = ExtractPeers(options.Cluster, nodeId, options.Network);

    using var jobQueue = new ThreadPerWorkerBackgroundJobQueue(options.Cluster.ClusterPeers.Length,
        options.Cluster.ClusterNodeId, Log.Logger.ForContext("SourceContext", "BackgroundJobQueue"), lifetime);
    using var consensusModule = CreateRaftConsensusModule(nodeId, peers, facade, jobQueue);
    using var requestAcceptor =
        new ExclusiveRequestAcceptor(consensusModule, lifetime, Log.ForContext("SourceContext", "RequestAcceptor"));
    using var connectionManager = new NodeConnectionManager(options.Cluster.ClusterListenHost,
        options.Cluster.ClusterListenPort,
        consensusModule, options.Network.ClientRequestTimeout,
        lifetime, Log.ForContext<NodeConnectionManager>());

    var taskFluxHostedService = new TaskFluxNodeHostedService(consensusModule, requestAcceptor, jobQueue,
        connectionManager, Log.ForContext<TaskFluxNodeHostedService>());

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
              .ConfigureHostConfiguration(builder =>
                   builder.AddEnvironmentVariables()
                          .AddCommandLine(args))
              .ConfigureAppConfiguration(builder =>
                   builder.AddEnvironmentVariables()
                          .AddCommandLine(args))
              .UseSerilog()
              .ConfigureServices((_, services) =>
               {
                   services.AddHostedService(_ => taskFluxHostedService);

                   services.AddSingleton<IRequestAcceptor>(requestAcceptor);
                   services.AddSingleton(options);
                   services.AddSingleton<IApplicationInfo>(sp => new ProxyApplicationInfo(consensusModule,
                       sp.GetRequiredService<ApplicationOptions>().Cluster.ClusterPeers));

                   services.AddNodeStateObserverHostedService(consensusModule);
                   services.AddTcpRequestModule(lifetime);
                   services.AddHttpRequestModule(lifetime);
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
    if (sigTermRegistration is not null)
    {
        sigTermRegistration.Dispose();
    }

    Log.CloseAndFlush();
}

Log.Information("Приложение закрывается");
return await lifetime.WaitReturnCode();

FileSystemPersistenceFacade InitializePersistence(PersistenceOptions options)
{
    return FileSystemPersistenceFacade.Initialize(options.WorkingDirectory, options.MaxLogFileSize);
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
    var applicationFactory = new TaskFluxApplicationFactory();

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
        peers[i] = TcpPeer.Create(currentNodeId, id, endpoint, networkOptions.ClientRequestTimeout,
            connectionErrorDelay,
            Log.ForContext("SourceContext", $"TcpPeer({id.Id})"));
    }

    // Все после текущего узла
    for (var i = currentNodeId.Id + 1; i < serverOptions.ClusterPeers.Length; i++)
    {
        var endpoint = serverOptions.ClusterPeers[i];
        var id = new NodeId(i);
        peers[i - 1] = TcpPeer.Create(currentNodeId, id, endpoint, networkOptions.ClientRequestTimeout,
            connectionErrorDelay,
            Log.ForContext("SourceContext", $"TcpPeer({id.Id})"));
    }

    return peers;
}

ApplicationOptions GetApplicationOptions()
{
    var root = new ConfigurationBuilder()
              .AddTaskFluxSource(args, "taskflux.settings.json")
              .Build();

    return ApplicationOptions.FromConfiguration(root);
}

bool TryValidateOptions(ApplicationOptions options)
{
    var results = new List<ValidationResult>();
    if (Validator.TryValidateObject(options, new ValidationContext(options), results, true))
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