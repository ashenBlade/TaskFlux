using System.ComponentModel.DataAnnotations;
using System.IO.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Core;
using TaskFlux.Application;
using TaskFlux.Consensus;
using TaskFlux.Consensus.Cluster;
using TaskFlux.Consensus.Cluster.Network;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.Persistence.Log;
using TaskFlux.Consensus.Persistence.Metadata;
using TaskFlux.Consensus.Persistence.Snapshot;
using TaskFlux.Consensus.Timers;
using TaskFlux.Core;
using TaskFlux.Core.Commands;
using TaskFlux.Host;
using TaskFlux.Host.Configuration;
using TaskFlux.Host.Infrastructure;

var lifetime = new HostApplicationLifetime();

var logLevelSwitch = new LoggingLevelSwitch();

Log.Logger = new LoggerConfiguration()
            .MinimumLevel.ControlledBy(logLevelSwitch)
            .Enrich.FromLogContext()
            .WriteTo.Console(outputTemplate:
                 "[{Timestamp:HH:mm:ss:ffff} {Level:u3}] ({SourceContext}) {Message}{NewLine}{Exception}")
            .Enrich.WithProperty("SourceContext", "Main") // Это для глобального логера
            .CreateLogger();

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
        options.Cluster.ClusterNodeId,
        Log.Logger.ForContext("SourceContext", "BackgroundJobQueue"), lifetime);
    using var consensusModule = CreateRaftConsensusModule(nodeId, peers, facade, jobQueue);
    using var requestAcceptor =
        new ExclusiveRequestAcceptor(consensusModule, lifetime, Log.ForContext("SourceContext", "RequestAcceptor"));
    using var connectionManager = new NodeConnectionManager(options.Cluster.ClusterListenHost,
        options.Cluster.ClusterListenPort,
        consensusModule, options.Network.ClientRequestTimeout,
        lifetime, Log.ForContext<NodeConnectionManager>());

    Console.CancelKeyPress += (_, args) =>
    {
        lifetime.Stop();
        args.Cancel = true;
    };

    var taskFluxHostedService = new TaskFluxNodeHostedService(consensusModule, requestAcceptor, jobQueue,
        connectionManager, Log.ForContext<TaskFluxNodeHostedService>());

    await RunNodeAsync(taskFluxHostedService,
        consensusModule,
        requestAcceptor);

    async Task RunNodeAsync(TaskFluxNodeHostedService node,
                            RaftConsensusModule<Command, Response> module,
                            IRequestAcceptor acceptor)
    {
        try
        {
            var host = new HostBuilder()
                      .ConfigureHostConfiguration(builder =>
                           builder.AddEnvironmentVariables()
                                  .AddCommandLine(args))
                      .ConfigureAppConfiguration(builder =>
                           builder.AddEnvironmentVariables()
                                  .AddCommandLine(args))
                      .ConfigureLogging(logging =>
                           logging.AddSerilog())
                      .ConfigureServices((_, services) =>
                       {
                           services.AddHostedService(_ => node);

                           services.AddSingleton(acceptor);
                           services.AddSingleton(options);
                           services.AddSingleton<IApplicationInfo>(sp => new ProxyApplicationInfo(module,
                               sp.GetRequiredService<ApplicationOptions>().Cluster.ClusterPeers));

                           services.AddNodeStateObserverHostedService(module);
                           services.AddTcpRequestModule(lifetime);
                           services.AddHttpRequestModule(lifetime);
                       })
                      .Build();

            await host.RunAsync(lifetime.Token);
            lifetime.Stop();
        }
        catch (Exception e)
        {
            Log.Fatal(e, "Во время работы приложения поймано необработанное исключение");
            lifetime.StopAbnormal();
        }
    }
}
catch (Exception e)
{
    Log.Fatal(e, "Необработанное исключение во время настройки сервера");
    lifetime.StopAbnormal();
}
finally
{
    Log.CloseAndFlush();
}

return await lifetime.WaitReturnCode();

// TODO: когда приложение закрывается, то нужно прекращать подключаться к узлам (Cancellation Token не работает)
FileSystemPersistenceFacade InitializePersistence(PersistenceOptions options)
{
    var dataDirPath = GetDataDirectory(options);

    var fs = new FileSystem();
    var dataDirectory = CreateDataDirectory();
    var dataDirectoryInfo = new DirectoryInfoWrapper(fs, dataDirectory);
    var fileLogStorage = CreateFileLogStorage();
    var metadataStorage = CreateMetadataStorage();
    var snapshotStorage = CreateSnapshotStorage(new DirectoryInfoWrapper(fs, dataDirectory));

    return new FileSystemPersistenceFacade(fileLogStorage, metadataStorage, snapshotStorage,
        Log.ForContext<FileSystemPersistenceFacade>(),
        maxLogFileSize: options.MaxLogFileSize);

    DirectoryInfo CreateDataDirectory()
    {
        var dir = new DirectoryInfo(Path.Combine(dataDirPath, "data"));
        if (!dir.Exists)
        {
            Log.Information("Директории для хранения данных не существует. Создаю новую - {Path}",
                dir.FullName);
            try
            {
                dir.Create();
            }
            catch (IOException e)
            {
                Log.Fatal(e, "Невозможно создать директорию для данных");
                throw;
            }
        }

        return dir;
    }

    SnapshotFile CreateSnapshotStorage(IDirectoryInfo dataDir)
    {
        var snapshotFile = new FileInfo(Path.Combine(dataDirectory.FullName, "raft.snapshot"));
        if (!snapshotFile.Exists)
        {
            Log.Information("Файл снапшота не обнаружен. Создаю новый - {Path}", snapshotFile.FullName);
            try
            {
                // Сразу закроем
                using var _ = snapshotFile.Create();
            }
            catch (Exception e)
            {
                Log.Fatal(e, "Ошибка при создании файла снапшота - {Path}", snapshotFile.FullName);
                throw;
            }
        }

        return SnapshotFile.Initialize(dataDir);
    }

    FileLog CreateFileLogStorage()
    {
        try
        {
            return FileLog.Initialize(dataDirectoryInfo);
        }
        catch (Exception e)
        {
            Log.Fatal(e, "Ошибка во время инициализации файла лога");
            throw;
        }
    }

    MetadataFile CreateMetadataStorage()
    {
        try
        {
            return MetadataFile.Initialize(dataDirectoryInfo);
        }
        catch (InvalidDataException invalidDataException)
        {
            Log.Fatal(invalidDataException, "Переданный файл метаданных был в невалидном состоянии");
            throw;
        }
        catch (Exception e)
        {
            Log.Fatal(e, "Ошибка во время инициализации файла метаданных");
            throw;
        }
    }

    string GetDataDirectory(PersistenceOptions persistenceOptions)
    {
        string workingDirectory;
        if (!string.IsNullOrWhiteSpace(persistenceOptions.WorkingDirectory))
        {
            workingDirectory = persistenceOptions.WorkingDirectory;
            Log.Debug("Указана рабочая директория: {WorkingDirectory}", workingDirectory);
        }
        else
        {
            var currentDirectory = Directory.GetCurrentDirectory();
            Log.Information("Директория данных не указана. Выставляю в рабочую директорию: {CurrentDirectory}",
                currentDirectory);
            workingDirectory = currentDirectory;
        }

        return workingDirectory;
    }
}

RaftConsensusModule<Command, Response> CreateRaftConsensusModule(NodeId nodeId,
                                                                 IPeer[] peers,
                                                                 FileSystemPersistenceFacade storage,
                                                                 IBackgroundJobQueue backgroundJobQueue)
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
        backgroundJobQueue, storage,
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
    if (persistence.Log.Count > 0)
    {
        Log.Information("Индекс закоммиченной команды: {CommitIndex}", persistence.Log.CommitIndex);
    }
}