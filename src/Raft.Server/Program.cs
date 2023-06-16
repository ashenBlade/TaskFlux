using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;
using Raft.CommandQueue;
using Raft.Core;
using Raft.Core.Peer;
using Raft.Core.StateMachine;
using Raft.JobQueue;
using Raft.Peer;
using Raft.Peer.Decorators;
using Raft.Server;
using Raft.Server.Infrastructure;
using Raft.Server.Options;
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

var options = configuration.Get<RaftServerOptions>() 
           ?? throw new Exception("Не найдено настроек");

ValidateOptions(options);

var nodeId = new PeerId(options.NodeId);

var requestTimeout = TimeSpan.FromSeconds(0.5);

var peers = options.Peers
                   .Select(peerInfo =>
                    {
                        var peerId = new PeerId(peerInfo.Id);
                        var tcpSocket = new RaftTcpSocket(peerInfo.Host, peerInfo.Port, nodeId, requestTimeout, options.ReceiveBufferSize, Log.ForContext("SourceContext", $"RaftTcpSocket-{peerId.Value}"));
                        var socket = new SingleAccessSocketDecorator(new NetworkExceptionWrapperDecorator(tcpSocket));
                        IPeer peer = new TcpPeer(peerId, socket, Log.ForContext("SourceContext", $"TcpPeer-{peerId.Value}"));
                        peer = new NetworkExceptionDelayPeerDecorator(peer, TimeSpan.FromMilliseconds(250));
                        return peer;
                    })
                   .ToArray();

Log.Logger.Information("Узлы кластера: {Peers}", options.Peers);

using var electionTimer = new RandomizedTimer(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5));
using var heartbeatTimer = new SystemTimersTimer( TimeSpan.FromSeconds(1) );

var log = new StubLog();
var jobQueue = new TaskJobQueue(Log.Logger.ForContext<TaskJobQueue>());

var node = new Node(nodeId, new PeerGroup(peers));

using var commandQueue = new ChannelCommandQueue();
using var raft = RaftStateMachine.Create(node, Log.ForContext<RaftStateMachine>(), electionTimer, heartbeatTimer, jobQueue, log, commandQueue);
var connectionManager = new ExternalConnectionManager(options.Host, options.Port, raft, Log.Logger.ForContext<ExternalConnectionManager>());
var server = new RaftStateObserver(raft, Log.Logger.ForContext<RaftStateObserver>());

using var cts = new CancellationTokenSource();

// ReSharper disable once AccessToDisposedClosure
Console.CancelKeyPress += (_, args) =>
{
    cts.Cancel();
    args.Cancel = true;
};

try
{
    Log.Logger.Information("Запускаю Election Timer");
    raft.ElectionTimer.Start();
    Log.Logger.Information("Запукаю фоновые задачи");
    await Task.WhenAll(
        server.RunAsync(cts.Token), 
        connectionManager.RunAsync(cts.Token), 
        commandQueue.RunAsync(cts.Token));
}
catch (Exception e)
{
    Log.Fatal(e, "Ошибка во время работы сервера");
}

void ValidateOptions(RaftServerOptions peersOptions)
{
    var errors = new List<ValidationResult>();
    if (!Validator.TryValidateObject(peersOptions, new ValidationContext(peersOptions), errors, true))
    {
        throw new Exception($"Найдены ошибки при валидации конфигурации: {string.Join(',', errors.Select(x => x.ErrorMessage))}");
    }
}