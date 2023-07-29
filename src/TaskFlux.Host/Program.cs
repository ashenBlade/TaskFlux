using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Net.Sockets;
using Consensus.CommandQueue;
using Consensus.CommandQueue.Channel;
using Consensus.Core;
using Consensus.Core.Log;
using Consensus.Core.State.LeaderState;
using Consensus.JobQueue;
using Consensus.Log;
using TaskFlux.Host;
using TaskFlux.Host.Infrastructure;
using TaskFlux.Host.Options;
using Consensus.Peer;
using Consensus.Peer.Decorators;
using Consensus.StateMachine;
using Consensus.StateMachine.JobQueue;
using Consensus.Storage.File;
using Consensus.Storage.File.Decorators;
using Consensus.Storage.File.Log;
using Consensus.Storage.File.Log.Decorators;
using Consensus.Timers;
using JobQueue.InMemory;
using JobQueue.PriorityQueue.StandardLibrary;
using Microsoft.Extensions.Configuration;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Serialization;
using TaskFlux.Core;
using TaskFlux.Host.Modules.BinaryRequest;
using TaskFlux.Host.Modules.HttpRequest;
using TaskFlux.Node;

Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .Enrich.FromLogContext()
            .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss.ffff} {Level:u3}] ({SourceContext}) {Message}{NewLine}{Exception}")
            .CreateLogger();
try
{
    var configuration = new ConfigurationBuilder()
                       .AddEnvironmentVariables()
                       .Build();

    var networkOptions = configuration.GetSection("NETWORK") is {} section 
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
                                  var client = new PacketClient(new Socket(AddressFamily.InterNetwork, SocketType.Stream,
                                      ProtocolType.Tcp));
                                  IPeer peer = new TcpPeer(client, endpoint, id, nodeId, networkOptions.ConnectionTimeout, networkOptions.RequestTimeout, Log.ForContext("SourceContext", $"TcpPeer({id.Value})"));
                                  peer = new ExclusiveAccessPeerDecorator(peer);
                                  peer = new NetworkExceptionDelayPeerDecorator(peer, TimeSpan.FromMilliseconds(250));
                                  return peer;
                              })
                             .ToArray();

    Log.Logger.Debug("Полученные узлы кластера: {Peers}", serverOptions.Peers);

    Log.Logger.Information("Открываю файл с метаданными");
    await using var metadataFileStream = OpenMetadataFile(serverOptions);
    Log.Logger.Information("Инициализирую хранилище метаданных");
    var (metadataStorage, _) = CreateMetadataStorage(metadataFileStream);

    Log.Information("Открываю файл с логом предзаписи");
    await using var fileStream = GetLogFileStream(serverOptions);
    Log.Information("Инициализирую хранилище лога предзаписи");
    var (storage, fileStorage) = CreateLogStorage(fileStream);
    var log = new StorageLog(storage);
    
    Log.Information("Создаю очередь машины состояний");
    var node = CreateNode();
    var appInfo = CreateApplicationInfo();
    var clusterInfo = CreateClusterInfo(serverOptions);
    var nodeInfo = CreateNodeInfo(serverOptions);
    var commandContext = new CommandContext(node, nodeInfo, appInfo, clusterInfo);
    
    RestoreState(log, fileStorage, commandContext);
    
    var jobQueueStateMachine = CreateJobQueueStateMachine(commandContext);

    using var electionTimer = new RandomizedTimer(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(3));
    using var heartbeatTimer = new SystemTimersTimer(TimeSpan.FromSeconds(1));
    using var commandQueue = new ChannelCommandQueue();

    using var raftConsensusModule = CreateRaftConsensusModule(nodeId, peers, electionTimer, heartbeatTimer, log, commandQueue, jobQueueStateMachine, metadataStorage);
    var consensusModule =
        new InfoUpdaterConsensusModuleDecorator<Command, Result>(raftConsensusModule, clusterInfo, nodeInfo);
    
    var connectionManager = new NodeConnectionManager(serverOptions.Host, serverOptions.Port, consensusModule, Log.Logger.ForContext<NodeConnectionManager>());
    var stateObserver = new NodeStateObserver(consensusModule, Log.Logger.ForContext<NodeStateObserver>());

    var httpModule = CreateHttpRequestModule(configuration);
    httpModule.AddHandler(HttpMethod.Post, "/command", new SubmitCommandRequestHandler(consensusModule, clusterInfo, Log.ForContext<SubmitCommandRequestHandler>()));
    
    var binaryRequestModule = CreateBinaryRequestModule(consensusModule, appInfo, clusterInfo, configuration);

    var nodeConnectionThread = new Thread(o =>
        {
            var (manager, token) = ( CancellableThreadParameter<NodeConnectionManager> ) o!;
            manager.Run(token);
        })
        {
            Priority = ThreadPriority.Highest, 
            Name = "Обработчик подключений узлов",
        };
    
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
        await Task.WhenAll(
            stateObserver.RunAsync(cts.Token),
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


BinaryRequestModule CreateBinaryRequestModule(IConsensusModule<Command, Result> consensusModule, IApplicationInfo applicationInfo, IClusterInfo clusterInfo, IConfiguration config)
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
        throw new Exception($"Найдены ошибки при валидации конфигурации: {string.Join(',', errors.Select(x => x.ErrorMessage))}");
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

FileStream GetLogFileStream(RaftServerOptions options)
{
    try
    {
        return File.Open(options.LogFile, FileMode.OpenOrCreate, FileAccess.ReadWrite);
    }
    catch (Exception e)
    {
        Log.Fatal(e, "Не удалось получить доступ к файлу лога {Filename}", options.LogFile);
        throw;
    }
}

( ILogStorage, FileLogStorage ) CreateLogStorage(Stream stream)
{
    stream = new BufferedStream(stream);
    FileLogStorage fileStorage;
    try
    {
        fileStorage = FileLogStorage.Initialize(stream);
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

    ILogStorage logStorage = new ExclusiveAccessLogStorageDecorator(fileStorage);
    logStorage = new LastLogEntryCachingFileLogStorageDecorator(logStorage);
    return ( logStorage, fileStorage );
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
        fileStorage = FileMetadataStorage.Initialize(stream, new Term(1), null);
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
    return new ProxyJobQueueStateMachine(context);
}

INode CreateNode()
{
    var jobQueue = new UnboundedJobQueue(new StandardLibraryPriorityQueue<int,byte[]>());
    return new SingleJobQueueNode(jobQueue);
}

RaftConsensusModule<Command, Result> CreateRaftConsensusModule(NodeId nodeId, IPeer[] peers, ITimer randomizedTimer, ITimer systemTimersTimer, ILog storageLog, ICommandQueue channelCommandQueue, IStateMachine<Command, Result> stateMachine, IMetadataStorage metadataStorage)
{
    var jobQueue = new TaskBackgroundJobQueue(Log.ForContext<TaskBackgroundJobQueue>());
    var logger = Log.ForContext("SourceContext", "Raft");
    var commandSerializer = new ProxyCommandSerializer();
    var requestQueueFactory = new ChannelRequestQueueFactory(storageLog);
    var peerGroup = new PeerGroup(peers);
    
    return RaftConsensusModule<Command, Result>.Create(nodeId, peerGroup, logger, randomizedTimer, systemTimersTimer, jobQueue, storageLog, channelCommandQueue, stateMachine, metadataStorage, commandSerializer, requestQueueFactory);
}

void RestoreState(StorageLog storageLog, FileLogStorage fileLogStorage, ICommandContext context)
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
    return new ApplicationInfo();
}

ClusterInfo CreateClusterInfo(RaftServerOptions options)
{
    return new ClusterInfo(new NodeId( options.NodeId ), options.Peers.Length);
}

NodeInfo CreateNodeInfo(RaftServerOptions options)
{
    return new NodeInfo(new NodeId(options.NodeId), NodeRole.Follower);
}