using System.ComponentModel.DataAnnotations;
using System.IO.Abstractions;
using Consensus.Application.TaskFlux;
using Consensus.JobQueue;
using Consensus.NodeProcessor;
using Consensus.Peer;
using Consensus.Raft;
using Consensus.Raft.Persistence;
using Consensus.Raft.Persistence.Log;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.Persistence.Snapshot;
using Consensus.Timers;
using Microsoft.Extensions.Configuration;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Core;
using TaskFlux.Host;
using TaskFlux.Host.Infrastructure;
using TaskFlux.Host.Modules;
using TaskFlux.Host.Modules.HttpRequest;
using TaskFlux.Host.Modules.SocketRequest;
using TaskFlux.Host.Options;
using TaskFlux.Host.RequestAcceptor;
using TaskFlux.Models;
using TaskFlux.Node;
using Utils.Network;

Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .Enrich.FromLogContext()
            .WriteTo.Console(outputTemplate:
                 "[{Timestamp:HH:mm:ss:ffff} {Level:u3}] ({SourceContext}) {Message}{NewLine}{Exception}")
            .CreateLogger();
try
{
    ThreadPool.SetMinThreads(1000, 1000);
    var configuration = new ConfigurationBuilder()
                       .AddEnvironmentVariables()
                       .AddJsonFile("taskflux.settings.json", optional: true)
                       .Build();

    var networkOptions = configuration.GetSection("NETWORK") is { } section
                      && section.Exists()
                             ? section.Get<NetworkOptions>() ?? NetworkOptions.Default
                             : NetworkOptions.Default;

    var serverOptions = configuration.Get<RaftServerOptions>()
                     ?? throw new Exception("Не найдено настроек сервера");

    ValidateOptions(serverOptions);

    var nodeId = new NodeId(serverOptions.NodeId);


    Log.Logger.Debug("Полученные узлы кластера: {Peers}", serverOptions.Peers);

    var facade = CreateStoragePersistenceFacade(serverOptions);
    var peers = ExtractPeers(serverOptions, nodeId, networkOptions);

    var appInfo = CreateApplicationInfo();
    var clusterInfo = CreateClusterInfo(serverOptions);
    var nodeInfo = CreateNodeInfo(serverOptions);

    using var raftConsensusModule = CreateRaftConsensusModule(nodeId, peers, facade, nodeInfo, appInfo, clusterInfo);

    var consensusModule =
        new InfoUpdaterRaftConsensusModuleDecorator<Command, Response>(raftConsensusModule, clusterInfo, nodeInfo);

    var connectionManager = new NodeConnectionManager(serverOptions.Host, serverOptions.Port, consensusModule,
        networkOptions.RequestTimeout,
        Log.Logger.ForContext<NodeConnectionManager>());

    var stateObserver = new NodeStateObserver(consensusModule, Log.Logger.ForContext<NodeStateObserver>());

    using var requestAcceptor =
        new ExclusiveRequestAcceptor(consensusModule, Log.ForContext("{SourceContext}", "RequestQueue"));

    var httpModule = CreateHttpRequestModule(configuration);
    httpModule.AddHandler(HttpMethod.Post, "/command",
        new SubmitCommandRequestHandler(requestAcceptor, clusterInfo, appInfo,
            Log.ForContext<SubmitCommandRequestHandler>()));

    var binaryRequestModule = CreateBinaryRequestModule(requestAcceptor, appInfo, clusterInfo, configuration);

    var nodeConnectionThread = new Thread(o =>
    {
        var (manager, token) = ( CancellableThreadParameter<NodeConnectionManager> ) o!;
        manager.Run(token);
    }) {Priority = ThreadPriority.Highest, Name = "Обработчик подключений узлов",};

    using var cts = new CancellationTokenSource();

    // ReSharper disable once AccessToDisposedClosure
    Console.CancelKeyPress += (_, args) =>
    {
        cts.Cancel();
        args.Cancel = true;
    };

    try
    {
        Log.Logger.Information("Запускаю таймер выборов");
        raftConsensusModule.Start();

        Log.Logger.Information("Запускаю менеджер подключений узлов");
        nodeConnectionThread.Start(new CancellableThreadParameter<NodeConnectionManager>(connectionManager, cts.Token));

        Log.Logger.Information("Запукаю фоновые задачи");
        await Task.WhenAll(stateObserver.RunAsync(cts.Token),
            httpModule.RunAsync(cts.Token),
            binaryRequestModule.RunAsync(cts.Token),
            Task.Run(() =>
            {
                // ReSharper disable once AccessToDisposedClosure
                var token = cts.Token;
                // ReSharper disable once AccessToDisposedClosure
                requestAcceptor.Start(token);
            }));
    }
    catch (Exception e)
    {
        Log.Fatal(e, "Ошибка во время работы сервера");
    }
    finally
    {
        cts.Cancel();
        nodeConnectionThread.Join();
    }
}
catch (Exception e)
{
    Log.Fatal(e, "Необработанное исключение во время настройки сервера");
}
finally
{
    Log.CloseAndFlush();
}

return;


SocketRequestModule CreateBinaryRequestModule(IRequestAcceptor requestAcceptor,
                                              IApplicationInfo applicationInfo,
                                              IClusterInfo clusterInfo,
                                              IConfiguration config)
{
    var options = GetOptions();

    try
    {
        Validator.ValidateObject(options, new ValidationContext(options), true);
    }
    catch (ValidationException ve)
    {
        Log.Error(ve, "Ошибка валидации настроек модуля бинарных запросов");
        throw;
    }

    return new SocketRequestModule(requestAcceptor,
        new StaticOptionsMonitor<SocketRequestModuleOptions>(options),
        clusterInfo,
        applicationInfo,
        Log.ForContext<SocketRequestModule>());

    SocketRequestModuleOptions GetOptions()
    {
        var section = config.GetSection("BINARY_REQUEST");
        if (!section.Exists())
        {
            return SocketRequestModuleOptions.Default;
        }

        return section.Get<SocketRequestModuleOptions>()
            ?? SocketRequestModuleOptions.Default;
    }
}

void ValidateOptions(RaftServerOptions serverOptions)
{
    var errors = new List<ValidationResult>();
    if (!Validator.TryValidateObject(serverOptions, new ValidationContext(serverOptions), errors, true))
    {
        throw new Exception(
            $"Найдены ошибки при валидации конфигурации: {string.Join(',', errors.Select(x => x.ErrorMessage))}");
    }
}

HttpRequestModule CreateHttpRequestModule(IConfiguration config)
{
    var httpModuleOptions = config.GetSection("HTTP")
                                  .Get<HttpRequestModuleOptions>()
                         ?? HttpRequestModuleOptions.Default;

    return new HttpRequestModule(httpModuleOptions.Port, Log.ForContext<HttpRequestModule>());
}

StoragePersistenceFacade CreateStoragePersistenceFacade(RaftServerOptions options)
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
                Log.Fatal(e, "Ошибка при создании файла снашпота - {Path}", snapshotFile.FullName);
                throw;
            }
        }

        return new FileSystemSnapshotStorage(new FileInfoWrapper(fs, snapshotFile),
            new DirectoryInfoWrapper(fs, tempDirectory), Log.ForContext("SourceContext", "SnapshotManager"));
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

    string GetDataDirectory(RaftServerOptions raftServerOptions)
    {
        string workingDirectory;
        if (!string.IsNullOrWhiteSpace(raftServerOptions.DataDirectory))
        {
            workingDirectory = raftServerOptions.DataDirectory;
            Log.Information("Указана директория данных: {WorkingDirectory}", workingDirectory);
        }
        else
        {
            Log.Information("Директория данных не указана. Выставляю в рабочую директорию");
            workingDirectory = Directory.GetCurrentDirectory();
        }

        return workingDirectory;
    }
}

RaftConsensusModule<Command, Response> CreateRaftConsensusModule(NodeId nodeId,
                                                                 IPeer[] peers,
                                                                 StoragePersistenceFacade storage,
                                                                 INodeInfo nodeInfo,
                                                                 IApplicationInfo applicationInfo,
                                                                 IClusterInfo clusterInfo)
{
    var jobQueue = new TaskBackgroundJobQueue(Log.ForContext<TaskBackgroundJobQueue>());
    var logger = Log.Logger.ForContext("SourceContext", "Raft");
    var commandSerializer = new TaskFluxDeltaExtractor();
    var peerGroup = new PeerGroup(peers);
    var timerFactory =
        new ThreadingTimerFactory(TimeSpan.FromMilliseconds(1500), TimeSpan.FromMilliseconds(2500),
            heartbeatTimeout: TimeSpan.FromMilliseconds(1000));

    return RaftConsensusModule<Command, Response>.Create(nodeId, peerGroup, logger, timerFactory,
        jobQueue, storage,
        commandSerializer, new TaskFluxApplicationFactory(nodeInfo, applicationInfo, clusterInfo));
}

ApplicationInfo CreateApplicationInfo()
{
    return new ApplicationInfo(QueueName.Default);
}

ClusterInfo CreateClusterInfo(RaftServerOptions options)
{
    return new ClusterInfo(new NodeId(options.NodeId), new NodeId(options.NodeId),
        options.Peers.Select(EndPointHelpers.ParseEndPoint));
}

NodeInfo CreateNodeInfo(RaftServerOptions options)
{
    return new NodeInfo(new NodeId(options.NodeId), NodeRole.Follower);
}

static IPeer[] ExtractPeers(RaftServerOptions serverOptions, NodeId currentNodeId, NetworkOptions networkOptions)
{
    var peers = new IPeer[serverOptions.Peers.Length - 1]; // Все кроме себя

    // Все до текущего узла
    for (int i = 0; i < currentNodeId.Id; i++)
    {
        var endpoint = EndPointHelpers.ParseEndPoint(serverOptions.Peers[i]);
        var id = new NodeId(i);
        IPeer peer = TcpPeer.Create(currentNodeId, id, endpoint, networkOptions.RequestTimeout,
            Log.ForContext("SourceContext", $"TcpPeer({id.Id})"));
        peer = new NetworkExceptionDelayPeerDecorator(peer, TimeSpan.FromMilliseconds(50));
        peers[i] = peer;
    }

    // Все после текущего узла
    for (int i = currentNodeId.Id + 1; i < serverOptions.Peers.Length; i++)
    {
        var endpoint = EndPointHelpers.ParseEndPoint(serverOptions.Peers[i]);
        var id = new NodeId(i);
        IPeer peer = TcpPeer.Create(currentNodeId, id, endpoint, networkOptions.RequestTimeout,
            Log.ForContext("SourceContext", $"TcpPeer({id.Id})"));
        peer = new NetworkExceptionDelayPeerDecorator(peer, TimeSpan.FromMilliseconds(50));
        peers[i - 1] = peer;
    }

    return peers;
}