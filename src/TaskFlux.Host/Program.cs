using System.ComponentModel.DataAnnotations;
using System.IO.Abstractions;
using System.Net;
using System.Runtime.InteropServices;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Serilog;
using Serilog.Core;
using TaskFlux.Application;
using TaskFlux.Application.Cluster;
using TaskFlux.Consensus;
using TaskFlux.Consensus.Cluster.Network;
using TaskFlux.Consensus.Timers;
using TaskFlux.Core;
using TaskFlux.Core.Commands;
using TaskFlux.Host;
using TaskFlux.Host.Configuration;
using TaskFlux.Host.Infrastructure;
using TaskFlux.Persistence;
using TaskFlux.Persistence.Log;
using TaskFlux.Persistence.Snapshot;
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

    using var jobQueue = new ThreadPerWorkerBackgroundJobQueue(options.Cluster.ClusterPeers.Length,
        options.Cluster.ClusterNodeId,
        Log.ForContext("SourceContext", "BackgroundJobQueue"), lifetime);
    using var consensusModule = CreateRaftConsensusModule(nodeId, peers, persistence, jobQueue);
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
              .UseSerilog()
              .ConfigureHostConfiguration(builder =>
                   builder.AddEnvironmentVariables("DOTNET_")
                          .AddCommandLine(args))
              .ConfigureAppConfiguration(builder =>
                   builder.AddEnvironmentVariables()
                          .AddCommandLine(args))
              .ConfigureWebHost(web =>
               {
                   web.Configure(app =>
                   {
                       app.UseRouting();
                       app.UseEndpoints(ep =>
                       {
                           ep.MapControllers();
                       });
                   });
                   web.UseKestrel();
               })
               // Настройка kestrel
              .ConfigureServices(sp =>
               {
                   sp.Configure<KestrelServerOptions>(kestrel =>
                   {
                       if (!string.IsNullOrWhiteSpace(options.Http.HttpListenAddress))
                       {
                           const int defaultHttpPort = 1606;
                           var address = options.Http.HttpListenAddress;
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
                                   Log.Information("Порт для HTTP запросов не указан. Выставляю в {DefaultHttpPort}",
                                       defaultHttpPort);
                                   port = int.Parse(parts[1]);
                               }
                               else
                               {
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
               })
              .ConfigureServices((_, services) =>
               {
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
    var dataDirectoryInfo = fs.DirectoryInfo.New(Path.Combine(options.WorkingDirectory, "data"));
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

bool TryValidateOptions(ApplicationOptions options)
{
    var results = new List<ValidationResult>();

    _ = Validator.TryValidateObject(options, new ValidationContext(options), results, true);

    if (options.Persistence.LogFileHardLimit < options.Persistence.LogFileSoftLimit)
    {
        results.Add(new ValidationResult(
            $"Жесткий предел размера файла не может быть меньше мягкого предела. Мягкий предел: {options.Persistence.LogFileSoftLimit}. Жесткий предел: {options.Persistence.LogFileHardLimit}",
            new[] {nameof(options.Persistence.LogFileHardLimit), nameof(options.Persistence.LogFileSoftLimit)}));
    }

    if (options.Http.HttpListenAddress is {Length: > 0} httpListenAddress)
    {
        try
        {
            _ = EndPointHelpers.ParseEndPoint(httpListenAddress, 0);
        }
        catch (ArgumentException ae)
        {
            results.Add(new ValidationResult($"Невалидный адрес для HTTP запросов: {ae.Message}",
                new[] {nameof(options.Http.HttpListenAddress)}));
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