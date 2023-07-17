using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Net.Sockets;
using JobQueue.InMemory;
using JobQueue.SortedQueue;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Raft.CommandQueue;
using Raft.Core;
using Raft.Core.Log;
using Raft.Core.Node;
using Raft.Host;
using Raft.Host.Infrastructure;
using Raft.Host.Modules.BinaryRequest;
using Raft.Host.Modules.HttpRequest;
using Raft.Host.Options;
using Raft.JobQueue;
using Raft.Log;
using Raft.Peer;
using Raft.Peer.Decorators;
using Raft.StateMachine;
using Raft.StateMachine.JobQueue;
using Raft.StateMachine.JobQueue.Serialization;
using Raft.Storage.File;
using Raft.Storage.File.Decorators;
using Raft.Storage.File.Log;
using Raft.Storage.File.Log.Decorators;
using Raft.Timers;
using Serilog;

// ReSharper disable CoVariantArrayConversion

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
    var jobQueueStateMachine = CreateJobQueueStateMachine();

    RestoreStateMachineState(log, fileStorage, jobQueueStateMachine);


    using var electionTimer = new RandomizedTimer(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(3));
    using var heartbeatTimer = new SystemTimersTimer(TimeSpan.FromSeconds(1));
    using var commandQueue = new ChannelCommandQueue();

    using var node = CreateRaftNode(nodeId, peers, electionTimer, heartbeatTimer, log, commandQueue, jobQueueStateMachine, metadataStorage);

    var connectionManager = new NodeConnectionManager(serverOptions.Host, serverOptions.Port, node, Log.Logger.ForContext<NodeConnectionManager>());
    var stateObserver = new NodeStateObserver(node, Log.Logger.ForContext<NodeStateObserver>());

    var httpModule = CreateHttpRequestModule(configuration);
    httpModule.AddHandler(HttpMethod.Post, "/command", new SubmitCommandRequestHandler(node, JobQueueRequestSerializer.Instance, Log.ForContext<SubmitCommandRequestHandler>()));
    
    var binaryRequestModule = CreateBinaryRequestModule(node, configuration);

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


BinaryRequestModule CreateBinaryRequestModule(INode node, IConfiguration config)
{
    var options = config.GetRequiredSection("BINARY_REQUEST")
                        .Get<BinaryRequestModuleOptions>() 
               ?? BinaryRequestModuleOptions.Default;

    try
    {
        Validator.ValidateObject(options, new ValidationContext(options), true);
    }
    catch (ValidationException ve)
    {
        Log.Error(ve, "Ошибка валидации настроек модуля бинарных запросов");
        throw;
    }

    return new BinaryRequestModule(node, new StaticOptionsMonitor<BinaryRequestModuleOptions>(options),
        Log.ForContext<BinaryRequestModule>());
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
                                  .Get<HttpModuleOptions>()
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

IStateMachine CreateJobQueueStateMachine()
{
    Log.Information("Создаю пустую очередь задач в памяти");
    var unboundedJobQueue = new UnboundedJobQueue(new PriorityQueueSortedQueue<int, byte[]>());
    Log.Information("Использую строковый десериализатор команд с кодировкой UTF-8");
    return new ProxyJobQueueStateMachine(unboundedJobQueue, new JobQueueCommandDeserializer(JobQueueRequestDeserializer.Instance, JobQueueResponseSerializer.Instance));
}

RaftNode CreateRaftNode(NodeId nodeId, IPeer[] peers, ITimer randomizedTimer, ITimer systemTimersTimer, ILog storageLog, ICommandQueue channelCommandQueue, IStateMachine stateMachine, IMetadataStorage metadataStorage)
{
    var jobQueue = new TaskJobQueue(Log.ForContext<TaskJobQueue>());
    return RaftNode.Create(nodeId, new PeerGroup(peers), Log.ForContext<RaftNode>(), randomizedTimer, systemTimersTimer, jobQueue, storageLog, channelCommandQueue, stateMachine, metadataStorage);
}

void RestoreStateMachineState(StorageLog storageLog, FileLogStorage fileLogStorage, IStateMachine stateMachine)
{
    if (fileLogStorage.Count == 0)
    {
        Log.Information("Пропускаю восстановление из лога: лог пуст");
        return;
    }
    
    try
    {
        Log.Information("Восстанавливаю предыдущее состояние из лога");
        storageLog.ApplyCommitted(stateMachine);
        Log.Debug("Состояние восстановлено. Применено {Count} записей", fileLogStorage.Count);
    }
    catch (Exception e)
    {
        Log.Fatal(e, "Ошибка во время восстановления лога");
        Log.CloseAndFlush();
        throw;
    }
}