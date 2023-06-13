using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;
using Raft.Core;
using Raft.JobQueue;
using Raft.Log;
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
                   .Select(peer =>
                    {
                        var peerId = new PeerId(peer.Id);
                        var tcpSocket = new RaftTcpSocket(peer.Host, peer.Port, nodeId, requestTimeout, options.ReceiveBufferSize, Log.ForContext("SourceContext", $"RaftTcpSocket-{peerId.Value}"));
                        var socket = new SingleAccessSocketDecorator(new NetworkExceptionWrapperDecorator(tcpSocket));
                        return new TcpPeer(peerId, socket, Log.ForContext("SourceContext", $"TcpPeer-{peerId.Value}"));
                    })
                   .ToArray();

Log.Logger.Information("Узлы кластера: {Peers}", options.Peers);

using var electionTimer = new RandomizedSystemTimersTimer(TimeSpan.FromSeconds(-1), TimeSpan.FromSeconds(1), options.ElectionTimeout );
using var heartbeatTimer = new SystemTimersTimer( TimeSpan.FromSeconds(1) );

var log = new StubLog();
var jobQueue = new TaskJobQueue(Log.Logger.ForContext<TaskJobQueue>());
var connectionManager = new ExternalConnectionsManager(options.Host, options.Port, Log.Logger.ForContext<ExternalConnectionsManager>());

var server = new RaftServer(nodeId, Log.Logger.ForContext<RaftServer>(), connectionManager, peers, log, jobQueue, electionTimer, heartbeatTimer);
using var cts = new CancellationTokenSource();

// ReSharper disable once AccessToDisposedClosure
Console.CancelKeyPress += (_, args) =>
{
    cts.Cancel();
    args.Cancel = true;
};

try
{
    await server.RunAsync(cts.Token);
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
        throw new Exception($"Найдены ошибки при валидации конфигурации: {errors.Select(x => x.ErrorMessage)}");
    }
}