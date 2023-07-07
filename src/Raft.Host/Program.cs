using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Net.Sockets;
using System.Text;
using JobQueue.InMemory;
using JobQueue.SortedQueue;
using Microsoft.Extensions.Configuration;
using Raft.CommandQueue;
using Raft.Core;
using Raft.Core.Log;
using Raft.Core.Node;
using Raft.Host;
using Raft.Host.HttpModule;
using Raft.Host.Infrastructure;
using Raft.Host.Options;
using Raft.JobQueue;
using Raft.Peer;
using Raft.Peer.Decorators;
using Raft.StateMachine;
using Raft.StateMachine.JobQueue;
using Raft.StateMachine.JobQueue.StringSerialization;
using Raft.StateMachine.Null;
using Raft.Storage.File;
using Raft.Storage.File.Decorators;
using Raft.Timers;
using Serilog;

// ReSharper disable CoVariantArrayConversion

Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .Enrich.FromLogContext()
            .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss.ffff} {Level:u3}] ({SourceContext}) {Message}{NewLine}{Exception}")
            .CreateLogger();

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

Log.Information("Открываю файл с логом предзаписи");
await using var fileStream = GetLogFileStream(serverOptions);
Log.Information("Инициализирую хранилище лога предзаписи");
var log = new StorageLog(CreateLogStorage(fileStream));


Log.Logger.Information("Открываю файл с метаданными");
await using var metadataFileStream = OpenMetadataFile(serverOptions);
Log.Logger.Information("Инициализирую хранилище метаданных");
var metadataStorage = CreateMetadataStorage(metadataFileStream);

using var electionTimer = new RandomizedTimer(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(3));
using var heartbeatTimer = new SystemTimersTimer(TimeSpan.FromSeconds(1));
using var commandQueue = new ChannelCommandQueue();
var stateMachine = CreateStateMachine();
var jobQueue = new TaskJobQueue(Log.Logger.ForContext<TaskJobQueue>());

using var node = RaftNode.Create(nodeId, new PeerGroup(peers), Log.ForContext<RaftNode>(), electionTimer, heartbeatTimer, jobQueue, log, commandQueue, stateMachine, metadataStorage);
var connectionManager = new NodeConnectionManager(serverOptions.Host, serverOptions.Port, node, Log.Logger.ForContext<NodeConnectionManager>());
var server = new NodeStateObserver(node, Log.Logger.ForContext<NodeStateObserver>());

var httpModule = CreateHttpRequestModule(configuration);

var nodeConnectionThread = new Thread(o =>
    {
        var (value, token) = ( CancellableThreadParameter<NodeConnectionManager> ) o!;
        value.Run(token);
    })
    {
        Priority = ThreadPriority.Highest, 
        Name = "Обработчик подключений узлов",
    };

httpModule.AddHandler(HttpMethod.Post, "/command", new SubmitCommandRequestHandler(node, Log.ForContext<SubmitCommandRequestHandler>()));
httpModule.AddHandler(HttpMethod.Get, "/metrics", new PrometheusRequestHandler(node));

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
        server.RunAsync(cts.Token),
        commandQueue.RunAsync(cts.Token),
        httpModule.RunAsync(cts.Token));
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

Log.CloseAndFlush();


void ValidateOptions( RaftServerOptions peersOptions)
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

    var module = new HttpRequestModule(httpModuleOptions.Port, Log.ForContext<HttpRequestModule>());

    return module;
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

ILogStorage CreateLogStorage(Stream stream)
{
    stream = new BufferedStream(stream);
    ILogStorage logStorage;
    try
    {
        logStorage = FileLogStorage.Initialize(stream);
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

    logStorage = new ExclusiveAccessLogStorageDecorator(logStorage);
    logStorage = new LastLogEntryCachingFileLogStorageDecorator(logStorage);
    return logStorage;
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

IMetadataStorage CreateMetadataStorage(Stream stream)
{
    stream = new BufferedStream(stream);
    IMetadataStorage storage;
    try
    {
        storage = FileMetadataStorage.Initialize(stream, new Term(1), null);
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
    
    storage = new CachingFileMetadataStorageDecorator(storage);
    storage = new ExclusiveAccessMetadataStorageDecorator(storage);
    
    return storage;
}

IStateMachine CreateStateMachine()
{
    Log.Information("Создаю пустую очередь задач в памяти");
    var unboundedJobQueue = new UnboundedJobQueue(new PriorityQueueSortedQueue<int, byte[]>());
    Log.Information("Использую строковый десериализатор команд с кодировкой UTF-8");
    var stringCommandDeserializer = new StringCommandDeserializer(Encoding.UTF8);
    return new ProxyJobQueueStateMachine(new SingleQueueJobQueueStateMachine(unboundedJobQueue), stringCommandDeserializer);
}