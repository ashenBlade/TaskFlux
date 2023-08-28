using System.ComponentModel.DataAnnotations;
using System.IO.Abstractions;
using System.Net;
using System.Net.Sockets;
using Consensus.CommandQueue;
using Consensus.CommandQueue.Channel;
using Consensus.JobQueue;
using Consensus.Peer;
using Consensus.Raft;
using Consensus.Raft.Persistence;
using Consensus.Raft.Persistence.Log;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.Persistence.Metadata.Decorators;
using Consensus.Raft.Persistence.Snapshot;
using Consensus.Raft.State.LeaderState;
using Consensus.StateMachine.TaskFlux;
using Consensus.Timers;
using JobQueue.Core;
using JobQueue.InMemory;
using JobQueue.PriorityQueue.StandardLibrary;
using JobQueue.Serialization;
using Microsoft.Extensions.Configuration;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Serialization;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Host;
using TaskFlux.Host.Helpers;
using TaskFlux.Host.Infrastructure;
using TaskFlux.Host.Modules.BinaryRequest;
using TaskFlux.Host.Modules.HttpRequest;
using TaskFlux.Host.Options;
using TaskFlux.Node;

Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .Enrich.FromLogContext()
            .WriteTo.Console(outputTemplate:
                 "[{Timestamp:HH:mm:ffff} {Level:u3}] ({SourceContext}) {Message}{NewLine}{Exception}")
            .CreateLogger();
try
{
    var configuration = new ConfigurationBuilder()
                       .AddEnvironmentVariables()
                       .Build();

    var networkOptions = configuration.GetSection("NETWORK") is { } section
                      && section.Exists()
                             ? section.Get<NetworkOptions>() ?? NetworkOptions.Default
                             : NetworkOptions.Default;

    var serverOptions = configuration.Get<RaftServerOptions>()
                     ?? throw new Exception("Не найдено настроек сервера");

    ValidateOptions(serverOptions);

    var nodeId = new NodeId(serverOptions.NodeId);

    var peers = serverOptions.Peers
                             .Select(p =>
                              {
                                  var endpoint = GetEndpoint(p.Host, p.Port);
                                  var id = new NodeId(p.Id);
                                  var client = new PacketClient(new Socket(AddressFamily.InterNetwork,
                                      SocketType.Stream,
                                      ProtocolType.Tcp));
                                  IPeer peer = new TcpPeer(client, endpoint, id, nodeId,
                                      networkOptions.ConnectionTimeout, networkOptions.RequestTimeout,
                                      Log.ForContext("SourceContext", $"TcpPeer({id.Id})"));
                                  peer = new NetworkExceptionDelayPeerDecorator(peer, TimeSpan.FromMilliseconds(250));
                                  return peer;
                              })
                             .ToArray();

    Log.Logger.Debug("Полученные узлы кластера: {Peers}", serverOptions.Peers);

    Log.Logger.Information("Открываю файл с метаданными");
    await using var metadataFileStream = OpenMetadataFile(serverOptions);
    Log.Logger.Information("Инициализирую хранилище метаданных");
    var (_, fileMetadataStorage) = CreateMetadataStorage(metadataFileStream);

    Log.Information("Инициализирую хранилище лога команд");
    var fileLogStorage = CreateLogStorage();
    var log = CreateStoragePersistenceManager(fileLogStorage, fileMetadataStorage);

    Log.Information("Создаю очередь машины состояний");
    var appInfo = CreateApplicationInfo();
    var node = CreateNode(appInfo);
    var clusterInfo = CreateClusterInfo(serverOptions);
    var nodeInfo = CreateNodeInfo(serverOptions);
    var commandContext = new CommandContext(node, nodeInfo, appInfo, clusterInfo);

    RestoreState(log, fileLogStorage, commandContext);

    var jobQueueStateMachine = CreateJobQueueStateMachine(commandContext);

    using var electionTimer = new RandomizedTimer(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(3));
    using var heartbeatTimer = new SystemTimersTimer(TimeSpan.FromSeconds(1));
    using var commandQueue = new ChannelCommandQueue();

    using var raftConsensusModule = CreateRaftConsensusModule(nodeId, peers, electionTimer, heartbeatTimer, log,
        commandQueue, jobQueueStateMachine, nodeInfo, appInfo, clusterInfo);
    var consensusModule =
        new InfoUpdaterConsensusModuleDecorator<Command, Result>(raftConsensusModule, clusterInfo, nodeInfo);

    var connectionManager = new NodeConnectionManager(serverOptions.Host, serverOptions.Port, consensusModule,
        Log.Logger.ForContext<NodeConnectionManager>());
    var stateObserver = new NodeStateObserver(consensusModule, Log.Logger.ForContext<NodeStateObserver>());

    var httpModule = CreateHttpRequestModule(configuration);
    httpModule.AddHandler(HttpMethod.Post, "/command",
        new SubmitCommandRequestHandler(consensusModule, clusterInfo, appInfo,
            Log.ForContext<SubmitCommandRequestHandler>()));

    var binaryRequestModule = CreateBinaryRequestModule(consensusModule, appInfo, clusterInfo, configuration);

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
        Log.Logger.Information("Запускаю менеджер подключений узлов");
        nodeConnectionThread.Start(new CancellableThreadParameter<NodeConnectionManager>(connectionManager, cts.Token));

        Log.Logger.Information("Запускаю Election Timer");
        electionTimer.Start();

        Log.Logger.Information("Запукаю фоновые задачи");
        await Task.WhenAll(stateObserver.RunAsync(cts.Token),
            commandQueue.RunAsync(cts.Token),
            httpModule.RunAsync(cts.Token),
            binaryRequestModule.RunAsync(cts.Token));
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
finally
{
    Log.CloseAndFlush();
}


BinaryRequestModule CreateBinaryRequestModule(IConsensusModule<Command, Result> consensusModule,
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

    return new BinaryRequestModule(consensusModule,
        new StaticOptionsMonitor<BinaryRequestModuleOptions>(options),
        clusterInfo,
        applicationInfo,
        Log.ForContext<BinaryRequestModule>());

    BinaryRequestModuleOptions GetOptions()
    {
        var section = config.GetSection("BINARY_REQUEST");
        if (!section.Exists())
        {
            return BinaryRequestModuleOptions.Default;
        }

        return section.Get<BinaryRequestModuleOptions>()
            ?? BinaryRequestModuleOptions.Default;
    }
}

void ValidateOptions(RaftServerOptions peersOptions)
{
    var errors = new List<ValidationResult>();
    if (!Validator.TryValidateObject(peersOptions, new ValidationContext(peersOptions), errors, true))
    {
        throw new Exception(
            $"Найдены ошибки при валидации конфигурации: {string.Join(',', errors.Select(x => x.ErrorMessage))}");
    }
}

HttpRequestModule CreateHttpRequestModule(IConfiguration config)
{
    var httpModuleOptions = config.GetRequiredSection("HTTP")
                                  .Get<HttpRequestModuleOptions>()
                         ?? throw new ApplicationException("Настройки для HTTP модуля не найдены");

    return new HttpRequestModule(httpModuleOptions.Port, Log.ForContext<HttpRequestModule>());
}

EndPoint GetEndpoint(string host, int port)
{
    if (IPAddress.TryParse(host, out var ip))
    {
        return new IPEndPoint(ip, port);
    }

    return new DnsEndPoint(host, port);
}

FileLogStorage CreateLogStorage()
{
    FileLogStorage fileStorage;

    try
    {
        fileStorage = FileLogStorage.InitializeFromFileSystem(GetConsensusDirectory());
    }
    catch (InvalidDataException invalidDataException)
    {
        Log.Fatal(invalidDataException, "Ошибка при инициализации лога из переданного файла");
        throw;
    }
    catch (Exception e)
    {
        Log.Fatal(e, "Неизвестная ошибка при инициализации лога");
        throw;
    }


    return fileStorage;


    IDirectoryInfo GetConsensusDirectory()
    {
        var cwd = Directory.GetCurrentDirectory();
        return new DirectoryInfoWrapper(new FileSystem(), new DirectoryInfo(Path.Combine(cwd, "consensus")));
    }
}

FileStream OpenMetadataFile(RaftServerOptions options)
{
    try
    {
        return File.Open(options.MetadataFile, FileMode.OpenOrCreate);
    }
    catch (Exception e)
    {
        Log.Fatal(e, "Ошибка во время доступа к файлу с метаданными по пути: {File}", options.MetadataFile);
        throw;
    }
}

( IMetadataStorage, FileMetadataStorage ) CreateMetadataStorage(Stream stream)
{
    stream = new BufferedStream(stream);
    FileMetadataStorage fileStorage;
    try
    {
        fileStorage = new FileMetadataStorage(stream, new Term(1), null);
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

    IMetadataStorage storage = new CachingFileMetadataStorageDecorator(fileStorage);
    storage = new ExclusiveAccessMetadataStorageDecorator(storage);

    return ( storage, fileStorage );
}

IStateMachine<Command, Result> CreateJobQueueStateMachine(ICommandContext context)
{
    Log.Information("Создаю пустую очередь задач в памяти");
    return new TaskFluxStateMachine(context, new FileJobQueueSnapshotSerializer(new PrioritizedJobQueueFactory()));
}

INode CreateNode(IApplicationInfo applicationInfo)
{
    var defaultJobQueue = PrioritizedJobQueue.CreateUnbounded(applicationInfo.DefaultQueueName,
        new StandardLibraryPriorityQueue<long, byte[]>());
    var jobQueueManager = new SimpleJobQueueManager(defaultJobQueue);
    return new TaskFluxNode(jobQueueManager);
}

RaftConsensusModule<Command, Result> CreateRaftConsensusModule(NodeId nodeId,
                                                               IPeer[] peers,
                                                               ITimer randomizedTimer,
                                                               ITimer systemTimersTimer,
                                                               StoragePersistenceFacade storage,
                                                               ICommandQueue channelCommandQueue,
                                                               IStateMachine<Command, Result> stateMachine,
                                                               INodeInfo nodeInfo,
                                                               IApplicationInfo appInfo,
                                                               IClusterInfo clusterInfo)
{
    var jobQueue = new TaskBackgroundJobQueue(Log.ForContext<TaskBackgroundJobQueue>());
    var logger = Log.ForContext("SourceContext", "Raft");
    var commandSerializer = new ProxyCommandCommandSerializer();
    var requestQueueFactory = new ChannelRequestQueueFactory(storage);
    var peerGroup = new PeerGroup(peers);
    var stateMachineFactory = new TaskFluxStateMachineFactory(nodeInfo, appInfo, clusterInfo);

    return RaftConsensusModule<Command, Result>.Create(nodeId, peerGroup, logger, randomizedTimer, systemTimersTimer,
        jobQueue, storage, channelCommandQueue, stateMachine, stateMachineFactory,
        commandSerializer,
        requestQueueFactory);
}

void RestoreState(StoragePersistenceFacade storageLog, FileLogStorage fileLogStorage, ICommandContext context)
{
    if (fileLogStorage.Count == 0)
    {
        Log.Information("Пропускаю восстановление из лога: лог пуст");
        return;
    }

    try
    {
        Log.Information("Восстанавливаю предыдущее состояние из лога");

        var notApplied = storageLog.GetNotApplied();

        foreach (var (_, payload) in notApplied)
        {
            var command = CommandSerializer.Instance.Deserialize(payload);
            command.Apply(context);
        }

        Log.Debug("Состояние восстановлено. Применено {Count} записей", fileLogStorage.Count);
    }
    catch (Exception e)
    {
        Log.Fatal(e, "Ошибка во время восстановления лога");
        Log.CloseAndFlush();
        throw;
    }
}

ApplicationInfo CreateApplicationInfo()
{
    return new ApplicationInfo(QueueName.Default);
}

ClusterInfo CreateClusterInfo(RaftServerOptions options)
{
    return new ClusterInfo(new NodeId(options.NodeId), options.Peers.Length);
}

NodeInfo CreateNodeInfo(RaftServerOptions options)
{
    return new NodeInfo(new NodeId(options.NodeId), NodeRole.Follower);
}

StoragePersistenceFacade CreateStoragePersistenceManager(FileLogStorage logStorage, FileMetadataStorage metadataStorage)
{
    var currentDirectory = Directory.GetCurrentDirectory();
    var raftDirectory = new DirectoryInfo(Path.Combine(currentDirectory, "consensus"));
    if (!raftDirectory.Exists)
    {
        if (File.Exists(raftDirectory.FullName))
        {
            throw new ApplicationException($"По пути {raftDirectory.FullName} лежит не диретория, а файл");
        }

        Log.Information("Директории с данными рафта нет. Создаю новую");
        raftDirectory.Create();
    }

    var fs = new FileSystem();
    var snapshotFile = new FileInfoWrapper(fs, new FileInfo(Path.Combine(raftDirectory.FullName, "raft.snapshot")));
    var tempDir = new DirectoryInfoWrapper(fs, CreateTemporarySnapshotFileDirectory(raftDirectory));
    var snapshotStorage = new FileSystemSnapshotStorage(snapshotFile, tempDir);

    return new StoragePersistenceFacade(logStorage, metadataStorage, snapshotStorage);

    static DirectoryInfo CreateTemporarySnapshotFileDirectory(DirectoryInfo raftDir)
    {
        var tempDir = new DirectoryInfo(Path.Combine(raftDir.FullName, "temp"));
        if (!tempDir.Exists)
        {
            if (File.Exists(tempDir.FullName))
            {
                throw new ApplicationException(
                    $"По пути директории {tempDir.FullName} для временных файлов рафта лежит не директория, а файл");
            }

            tempDir.Create();
        }

        return tempDir;
    }
}