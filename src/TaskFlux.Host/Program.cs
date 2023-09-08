using System.ComponentModel.DataAnnotations;
using System.IO.Abstractions;
using System.Net;
using System.Net.Sockets;
using Consensus.JobQueue;
using Consensus.NodeProcessor;
using Consensus.Peer;
using Consensus.Raft;
using Consensus.Raft.Persistence;
using Consensus.Raft.Persistence.Log;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.Persistence.Snapshot;
using Consensus.Timers;
using JobQueue.Core;
using Microsoft.Extensions.Configuration;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Serialization;
using TaskFlux.Core;
using TaskFlux.Host;
using TaskFlux.Host.Infrastructure;
using TaskFlux.Host.Modules;
using TaskFlux.Host.Modules.HttpRequest;
using TaskFlux.Host.Modules.SocketRequest;
using TaskFlux.Host.Options;
using TaskFlux.Host.RequestAcceptor;
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

    var facade = CreateStoragePersistenceFacade();

    Log.Information("Создаю очередь машины состояний");
    var appInfo = CreateApplicationInfo();
    var clusterInfo = CreateClusterInfo(serverOptions);
    var nodeInfo = CreateNodeInfo(serverOptions);
    var factory = new TaskFluxStateMachineFactory(nodeInfo, appInfo, clusterInfo);
    var taskFluxStateMachine = RestoreState(facade, factory);

    using var raftConsensusModule = CreateRaftConsensusModule(nodeId, peers, facade, taskFluxStateMachine, factory);

    var consensusModule =
        new InfoUpdaterConsensusModuleDecorator<Command, Result>(raftConsensusModule, clusterInfo, nodeInfo);

    var connectionManager = new NodeConnectionManager(serverOptions.Host, serverOptions.Port, consensusModule,
        Log.Logger.ForContext<NodeConnectionManager>());

    var stateObserver = new NodeStateObserver(consensusModule, Log.Logger.ForContext<NodeStateObserver>());

    var httpModule = CreateHttpRequestModule(configuration);
    httpModule.AddHandler(HttpMethod.Post, "/command",
        new SubmitCommandRequestHandler(consensusModule, clusterInfo, appInfo,
            Log.ForContext<SubmitCommandRequestHandler>()));

    using var requestAcceptor =
        new ExclusiveRequestAcceptor(consensusModule, Log.ForContext("{SourceContext}", "RequestQueue"));

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
        Log.Logger.Information("Запускаю менеджер подключений узлов");
        nodeConnectionThread.Start(new CancellableThreadParameter<NodeConnectionManager>(connectionManager, cts.Token));

        Log.Logger.Information("Запукаю фоновые задачи");
        await Task.WhenAll(stateObserver.RunAsync(cts.Token),
            httpModule.RunAsync(cts.Token),
            binaryRequestModule.RunAsync(cts.Token),
            new Task(() =>
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

StoragePersistenceFacade CreateStoragePersistenceFacade()
{
    var fs = new FileSystem();
    var consensusDirectory = new DirectoryInfo(Path.Combine(Directory.GetCurrentDirectory(), "consensus"));
    if (!consensusDirectory.Exists)
    {
        Log.Information("Директории для хранения данных не существует. Создаю новую - {Path}",
            consensusDirectory.FullName);
        try
        {
            consensusDirectory.Create();
        }
        catch (IOException e)
        {
            Log.Fatal(e, "Невозможно создать директорию для данных");
            throw;
        }
    }

    var tempDirectory = CreateTemporaryDirectory();

    var fileLogStorage = CreateFileLogStorage();
    var metadataStorage = CreateMetadataStorage();
    var snapshotStorage = CreateSnapshotStorage();

    return new StoragePersistenceFacade(fileLogStorage, metadataStorage, snapshotStorage);

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
            new DirectoryInfoWrapper(fs, tempDirectory));
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
}

RaftConsensusModule<Command, Result> CreateRaftConsensusModule(NodeId nodeId,
                                                               IPeer[] peers,
                                                               StoragePersistenceFacade storage,
                                                               IStateMachine<Command, Result> stateMachine,
                                                               TaskFluxStateMachineFactory stateMachineFactory)
{
    var jobQueue = new TaskBackgroundJobQueue(Log.ForContext<TaskBackgroundJobQueue>());
    var logger = Log.ForContext("SourceContext", "Raft");
    var commandSerializer = new ProxyCommandCommandSerializer();
    var peerGroup = new PeerGroup(peers);
    var timerFactory =
        new ThreadingTimerFactory(TimeSpan.FromMilliseconds(150), TimeSpan.FromMilliseconds(300),
            heartbeatTimeout: TimeSpan.FromMilliseconds(100));

    // TODO: ручной запуск таймера
    return RaftConsensusModule<Command, Result>.Create(nodeId, peerGroup, logger, timerFactory,
        jobQueue, storage, stateMachine, stateMachineFactory,
        commandSerializer);
}

IStateMachine<Command, Result> RestoreState(StoragePersistenceFacade facade, TaskFluxStateMachineFactory factory)
{
    // 1. Восстановить данные из снапшота, если есть, иначе инициализировать пустым
    // 2. Применить команды из лога
    IStateMachine<Command, Result> stateMachine;
    if (facade.TryGetSnapshot(out var snapshot))
    {
        Log.Information("Обнаружен существующий файл снашпота. Восстанавливаю состояние");
        try
        {
            stateMachine = factory.Restore(snapshot);
        }
        catch (Exception e)
        {
            Log.Fatal(e, "Ошибка во время восстановления состояния из снапшота");
            throw;
        }
    }
    else
    {
        stateMachine = factory.CreateEmpty();
    }

    try
    {
        Log.Information("Восстанавливаю предыдущее состояние из лога");

        var notApplied = facade.GetNotApplied();
        if (notApplied.Count > 0)
        {
            foreach (var (_, payload) in notApplied)
            {
                var command = CommandSerializer.Instance.Deserialize(payload);
                stateMachine.ApplyNoResponse(command);
            }

            Log.Debug("Состояние восстановлено. Применено {Count} записей", notApplied.Count);
        }
    }
    catch (Exception e)
    {
        Log.Fatal(e, "Ошибка во время восстановления лога");
        Log.CloseAndFlush();
        throw;
    }

    return stateMachine;
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