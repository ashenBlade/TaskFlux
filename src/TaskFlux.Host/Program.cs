using System.ComponentModel.DataAnnotations;
using System.IO.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
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

Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .Enrich.FromLogContext()
            .WriteTo.Console(outputTemplate:
                 "[{Timestamp:HH:mm:ss:ffff} {Level:u3}] ({SourceContext}) {Message}{NewLine}{Exception}")
            .CreateLogger();

try
{
    var options = GetApplicationOptions();

    if (!TryValidateOptions(options))
    {
        return 1;
    }

    var nodeId = new NodeId(options.Cluster.ClusterNodeId);

    var facade = InitializeStorage(options.Cluster);
    var peers = ExtractPeers(options.Cluster, nodeId, options.Network);

    using var jobQueue = new ThreadPerWorkerBackgroundJobQueue(options.Cluster.ClusterPeers.Length,
        options.Cluster.ClusterNodeId,
        Log.Logger.ForContext("SourceContext", "BackgroundJobQueue"));
    using var consensusModule = CreateRaftConsensusModule(nodeId, peers, facade, jobQueue);
    using var requestAcceptor =
        new ExclusiveRequestAcceptor(consensusModule, Log.ForContext("SourceContext", "RequestAcceptor"));
    using var connectionManager = new NodeConnectionManager(options.Cluster.ClusterListenHost,
        options.Cluster.ClusterListenPort,
        consensusModule, options.Network.ClientRequestTimeout, Log.ForContext<NodeConnectionManager>());
    using var applicationLifetimeCts = new CancellationTokenSource();

    Console.CancelKeyPress += (_, args) =>
    {
        // ReSharper disable once AccessToDisposedClosure
        applicationLifetimeCts.Cancel();
        args.Cancel = true;
    };

    var taskFluxHostedService = new TaskFluxNodeHostedService(consensusModule, requestAcceptor, jobQueue,
        connectionManager, Log.ForContext<TaskFluxNodeHostedService>());

    await RunNodeAsync(taskFluxHostedService,
        consensusModule,
        requestAcceptor,
        applicationLifetimeCts.Token);

    async Task RunNodeAsync(TaskFluxNodeHostedService node,
                            RaftConsensusModule<Command, Response> module,
                            IRequestAcceptor acceptor,
                            CancellationToken token)
    {
        var host = new HostBuilder()
                  .ConfigureHostConfiguration(builder =>
                       builder.AddEnvironmentVariables()
                              .AddCommandLine(args))
                  .ConfigureAppConfiguration(builder =>
                       builder.AddEnvironmentVariables()
                              .AddCommandLine(args))
                  .ConfigureLogging(logging => logging.AddSerilog())
                  .ConfigureServices((_, services) =>
                   {
                       services.AddHostedService(_ => node);

                       services.AddSingleton(acceptor);
                       services.AddSingleton(options);
                       services.AddSingleton<IApplicationInfo>(sp => new ProxyApplicationInfo(module,
                           sp.GetRequiredService<ApplicationOptions>().Cluster.ClusterPeers));

                       services.AddNodeStateObserverHostedService(module);
                       services.AddTcpRequestModule();
                       services.AddHttpRequestModule();
                   })
                  .UseConsoleLifetime()
                  .Build();

        await host.RunAsync(token);
    }
}
catch (Exception e)
{
    Log.Fatal(e, "Необработанное исключение во время настройки сервера");
    return 1;
}
finally
{
    Log.CloseAndFlush();
}

return 0;

StoragePersistenceFacade InitializeStorage(ClusterOptions options)
{
    var dataDirectory = GetDataDirectory(options);

    var fs = new FileSystem();
    var consensusDirectory = CreateConsensusDirectory();

    var tempDirectory = CreateTemporaryDirectory();

    var fileLogStorage = CreateFileLogStorage();
    var metadataStorage = CreateMetadataStorage();
    var snapshotStorage = CreateSnapshotStorage();

    return new StoragePersistenceFacade(fileLogStorage, metadataStorage, snapshotStorage,
        maxLogFileSize: 1024 /* 1 Кб */);

    DirectoryInfo CreateTemporaryDirectory()
    {
        var temporary = new DirectoryInfo(Path.Combine(consensusDirectory.FullName, "temporary"));
        if (!temporary.Exists)
        {
            Log.Information("Директории для временных файлов не найдено. Создаю новую - {Path}", temporary.FullName);
            try
            {
                temporary.Create();
                return temporary;
            }
            catch (Exception e)
            {
                Log.Fatal(e, "Ошибка при создании директории для временных файлов в {Path}", temporary.FullName);
                throw;
            }
        }

        return temporary;
    }

    DirectoryInfo CreateConsensusDirectory()
    {
        var dir = new DirectoryInfo(Path.Combine(dataDirectory, "consensus"));
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

    FileSystemSnapshotStorage CreateSnapshotStorage()
    {
        var snapshotFile = new FileInfo(Path.Combine(consensusDirectory.FullName, "raft.snapshot"));
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

        return new FileSystemSnapshotStorage(new FileInfoWrapper(fs, snapshotFile),
            new DirectoryInfoWrapper(fs, tempDirectory),
            Log.ForContext("SourceContext", "SnapshotManager"));
    }

    FileLogStorage CreateFileLogStorage()
    {
        try
        {
            return FileLogStorage.InitializeFromFileSystem(new DirectoryInfoWrapper(fs, consensusDirectory),
                new DirectoryInfoWrapper(fs, tempDirectory));
        }
        catch (Exception e)
        {
            Log.Fatal(e, "Ошибка во время инициализации файла лога");
            throw;
        }
    }

    FileMetadataStorage CreateMetadataStorage()
    {
        var metadataFile = new FileInfo(Path.Combine(consensusDirectory.FullName, "raft.metadata"));
        FileStream fileStream;
        if (!metadataFile.Exists)
        {
            Log.Information("Файла метаданных не обнаружен. Создаю новый - {Path}", metadataFile.FullName);
            try
            {
                fileStream = metadataFile.Create();
            }
            catch (Exception e)
            {
                Log.Fatal(e, "Не удалось создать новый файл метаданных -  {Path}", metadataFile.FullName);
                throw;
            }
        }
        else
        {
            try
            {
                fileStream = metadataFile.Open(FileMode.Open, FileAccess.ReadWrite);
            }
            catch (Exception e)
            {
                Log.Fatal(e, "Ошибка при открытии файла метаданных");
                throw;
            }
        }

        try
        {
            return new FileMetadataStorage(fileStream, new Term(1), null);
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

    string GetDataDirectory(ClusterOptions raftServerOptions)
    {
        string workingDirectory;
        if (!string.IsNullOrWhiteSpace(raftServerOptions.ClusterDataDirectory))
        {
            workingDirectory = raftServerOptions.ClusterDataDirectory;
            Log.Debug("Указана директория данных: {WorkingDirectory}", workingDirectory);
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
                                                                 StoragePersistenceFacade storage,
                                                                 IBackgroundJobQueue backgroundJobQueue)
{
    var logger = Log.Logger.ForContext("SourceContext", "Raft");
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